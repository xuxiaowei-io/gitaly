package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// ErrObjectNotFound is returned in case an object could not be found.
var ErrObjectNotFound = errors.New("object not found")

// InvalidObjectError is returned when trying to get an object id that is invalid or does not exist.
type InvalidObjectError string

func (err InvalidObjectError) Error() string { return fmt.Sprintf("invalid object %q", string(err)) }

// ReadObjectInfo attempts to read the object info based on a revision.
func (repo *Repo) ReadObjectInfo(ctx context.Context, rev git.Revision) (*catfile.ObjectInfo, error) {
	infoReader, cleanup, err := repo.catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("getting object info reader: %w", err)
	}
	defer cleanup()

	objectInfo, err := infoReader.Info(ctx, rev)
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return nil, InvalidObjectError(rev)
		}
		return nil, fmt.Errorf("getting object info: %w", err)
	}

	return objectInfo, nil
}

// ReadObject reads an object from the repository's object database. InvalidObjectError
// is returned if the oid does not refer to a valid object.
func (repo *Repo) ReadObject(ctx context.Context, oid git.ObjectID) ([]byte, error) {
	objectReader, cancel, err := repo.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("create object reader: %w", err)
	}
	defer cancel()

	object, err := objectReader.Object(ctx, oid.Revision())
	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return nil, InvalidObjectError(oid.String())
		}
		return nil, fmt.Errorf("get object from reader: %w", err)
	}

	data, err := io.ReadAll(object)
	if err != nil {
		return nil, fmt.Errorf("read object from reader: %w", err)
	}

	return data, nil
}

type readCommitConfig struct {
	withTrailers bool
}

// ReadCommitOpt is an option for ReadCommit.
type ReadCommitOpt func(*readCommitConfig)

// WithTrailers will cause ReadCommit to parse commit trailers.
func WithTrailers() ReadCommitOpt {
	return func(cfg *readCommitConfig) {
		cfg.withTrailers = true
	}
}

// ReadCommit reads the commit specified by the given revision. If no such
// revision exists, it will return an ErrObjectNotFound error.
func (repo *Repo) ReadCommit(ctx context.Context, revision git.Revision, opts ...ReadCommitOpt) (*gitalypb.GitCommit, error) {
	var cfg readCommitConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	objectReader, cancel, err := repo.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, err
	}
	defer cancel()

	var commit *gitalypb.GitCommit
	if cfg.withTrailers {
		commit, err = catfile.GetCommitWithTrailers(ctx, repo.gitCmdFactory, repo, objectReader, revision)
	} else {
		commit, err = catfile.GetCommit(ctx, objectReader, revision)
	}

	if err != nil {
		if errors.As(err, &catfile.NotFoundError{}) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return commit, nil
}

// InvalidCommitError is returned when the revision does not point to a valid commit object.
type InvalidCommitError git.Revision

func (err InvalidCommitError) Error() string {
	return fmt.Sprintf("invalid commit: %q", string(err))
}

// IsAncestor returns whether the parent is an ancestor of the child. InvalidCommitError is returned
// if either revision does not point to a commit in the repository.
func (repo *Repo) IsAncestor(ctx context.Context, parent, child git.Revision) (bool, error) {
	const notValidCommitName = "fatal: Not a valid commit name"

	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name:  "merge-base",
			Flags: []git.Option{git.Flag{Name: "--is-ancestor"}},
			Args:  []string{parent.String(), child.String()},
		},
		git.WithStderr(stderr),
	); err != nil {
		status, ok := command.ExitStatus(err)
		if ok && status == 1 {
			return false, nil
		} else if ok && strings.HasPrefix(stderr.String(), notValidCommitName) {
			commitOID := strings.TrimSpace(strings.TrimPrefix(stderr.String(), notValidCommitName))
			return false, InvalidCommitError(commitOID)
		}

		return false, fmt.Errorf("determine ancestry: %w, stderr: %q", err, stderr)
	}

	return true, nil
}

// BadObjectError is returned when attempting to walk a bad object.
type BadObjectError struct {
	// ObjectID is the object id of the object that was bad.
	ObjectID git.ObjectID
}

// Error returns the error message.
func (err BadObjectError) Error() string {
	return fmt.Sprintf("bad object %q", err.ObjectID)
}

// ObjectReadError is returned when reading an object fails.
type ObjectReadError struct {
	// ObjectID is the object id of the object that git failed to read
	ObjectID git.ObjectID
}

// Error returns the error message.
func (err ObjectReadError) Error() string {
	return fmt.Sprintf("failed reading object %q", err.ObjectID)
}

var (
	regexpBadObjectError  = regexp.MustCompile(`^fatal: bad object ([[:xdigit:]]*)\n$`)
	regexpObjectReadError = regexp.MustCompile(`^error: Could not read ([[:xdigit:]]*)\n`)
)

// WalkUnreachableObjects walks the object graph starting from heads and writes to the output object IDs
// that are included in the walk but unreachable from any of the repository's references. Heads should
// return object IDs separated with a newline. Output is object IDs separated by newlines.
func (repo *Repo) WalkUnreachableObjects(ctx context.Context, heads io.Reader, output io.Writer) error {
	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name: "rev-list",
			Flags: []git.Option{
				git.Flag{Name: "--objects"},
				git.Flag{Name: "--not"},
				git.Flag{Name: "--all"},
				git.Flag{Name: "--stdin"},
			},
		},
		git.WithStdin(heads),
		git.WithStdout(output),
		git.WithStderr(&stderr),
	); err != nil {
		if matches := regexpBadObjectError.FindSubmatch(stderr.Bytes()); len(matches) > 1 {
			return BadObjectError{ObjectID: git.ObjectID(matches[1])}
		}

		if matches := regexpObjectReadError.FindSubmatch(stderr.Bytes()); len(matches) > 1 {
			return ObjectReadError{ObjectID: git.ObjectID(matches[1])}
		}

		return structerr.New("rev-list: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// PackObjects takes in object IDs separated by newlines. It packs the objects into a pack file and
// writes it into the output.
func (repo *Repo) PackObjects(ctx context.Context, objectIDs io.Reader, output io.Writer) error {
	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name: "pack-objects",
			Flags: []git.Option{
				git.Flag{Name: "-q"},
				git.Flag{Name: "--stdout"},
			},
		},
		git.WithStdin(objectIDs),
		git.WithStderr(&stderr),
		git.WithStdout(output),
	); err != nil {
		return structerr.New("pack objects: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}

// UnpackObjects unpacks the objects from the pack file to the repository's object database.
func (repo *Repo) UnpackObjects(ctx context.Context, packFile io.Reader) error {
	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name: "unpack-objects",
			Flags: []git.Option{
				git.Flag{Name: "-q"},
			},
		},
		git.WithStdin(packFile),
		git.WithStderr(stderr),
	); err != nil {
		return structerr.New("unpack objects: %w", err).WithMetadata("stderr", stderr.String())
	}

	return nil
}
