package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// ErrMissingTree indicates a missing tree when attemping to write a commit
var ErrMissingTree = errors.New("missing tree")

// WriteCommitConfig contains fields for writing a commit
type WriteCommitConfig struct {
	Reference          string
	Parents            []git.ObjectID
	AuthorDate         time.Time
	AuthorName         string
	AuthorEmail        string
	CommitterName      string
	CommitterEmail     string
	CommitterDate      time.Time
	Message            string
	TreeEntries        []TreeEntry
	TreeID             git.ObjectID
	AlternateObjectDir string
}

// WriteCommitOption is an option which can be passed to WriteCommit.
type WriteCommitOption func(*WriteCommitConfig)

// WithReference is an option for WriteCommit which will cause it to update the given reference to
// point to the new commit. This function requires the fully-qualified reference name.
func WithReference(reference string) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.Reference = reference
	}
}

// WithBranch is an option for WriteCommit which will cause it to update the given branch name to
// the new commit.
func WithBranch(branch string) WriteCommitOption {
	return WithReference("refs/heads/" + branch)
}

// WithMessage is an option for WriteCommit which will set the commit message.
func WithMessage(message string) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.Message = message
	}
}

// WithParents is an option for WriteCommit which will set the parent OIDs of the resulting commit.
func WithParents(parents ...git.ObjectID) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		if parents != nil {
			cfg.Parents = parents
		} else {
			// We're explicitly initializing parents here such that we can discern the
			// case where the commit should be created with no parents.
			cfg.Parents = []git.ObjectID{}
		}
	}
}

// WithTreeEntries is an option for WriteCommit which will cause it to create a new tree and use it
// as root tree of the resulting commit.
func WithTreeEntries(entries ...TreeEntry) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.TreeEntries = entries
	}
}

// WithTree is an option for WriteCommit which will cause it to use the given object ID as the root
// tree of the resulting commit.
// as root tree of the resulting commit.
func WithTree(treeID git.ObjectID) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.TreeID = treeID
	}
}

// WithAuthorName is an option for WriteCommit which will set the author name.
func WithAuthorName(name string) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.AuthorName = name
	}
}

// WithAuthorDate is an option for WriteCommit which will set the author date.
func WithAuthorDate(date time.Time) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.AuthorDate = date
	}
}

// WithCommitterName is an option for WriteCommit which will set the committer name.
func WithCommitterName(name string) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.CommitterName = name
	}
}

// WithCommitterDate is an option for WriteCommit which will set the committer date.
func WithCommitterDate(date time.Time) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.CommitterDate = date
	}
}

// WithAlternateObjectDirectory will cause the commit to be written into the given alternate object
// directory. This can either be an absolute path or a relative path. In the latter case the path
// is considered to be relative to the repository path.
func WithAlternateObjectDirectory(alternateObjectDir string) WriteCommitOption {
	return func(cfg *WriteCommitConfig) {
		cfg.AlternateObjectDir = alternateObjectDir
	}
}

// WriteCommit writes a new commit into the target repository.
func (repo *Repo) WriteCommit(ctx context.Context, cfg WriteCommitConfig) (git.ObjectID, error) {
	var tree git.ObjectID
	var err error

	if cfg.TreeID == "" {
		return "", ErrMissingTree
	}

	tree = cfg.TreeID

	if cfg.AuthorDate.IsZero() {
		cfg.AuthorDate = time.Now()
	}

	if cfg.CommitterDate.IsZero() {
		cfg.CommitterDate = time.Now()
	}

	// Use 'commit-tree' instead of 'commit' because we are in a bare
	// repository. What we do here is the same as "commit -m message
	// --allow-empty".
	commitArgs := []string{string(tree)}

	repoPath, err := repo.Path()
	if err != nil {
		return "", err
	}

	var env []string
	if cfg.AlternateObjectDir != "" {
		if !filepath.IsAbs(cfg.AlternateObjectDir) {
			return "", errors.New("alternate object directory must be an absolute path")
		}

		if err := os.MkdirAll(cfg.AlternateObjectDir, 0o755); err != nil {
			return "", err
		}

		env = append(env,
			fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", cfg.AlternateObjectDir),
			fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", filepath.Join(repoPath, "objects")),
		)
	}

	env = append(env,
		fmt.Sprintf("GIT_AUTHOR_DATE=%s", cfg.AuthorDate.String()),
		fmt.Sprintf("GIT_AUTHOR_NAME=%s", cfg.AuthorName),
		fmt.Sprintf("GIT_AUTHOR_EMAIL=%s", cfg.AuthorEmail),
		fmt.Sprintf("GIT_COMMITTER_DATE=%s", cfg.CommitterDate.String()),
		fmt.Sprintf("GIT_COMMITTER_NAME=%s", cfg.CommitterName),
		fmt.Sprintf("GIT_COMMITTER_EMAIL=%s", cfg.CommitterEmail),
	)

	var flags []git.Option

	for _, parent := range cfg.Parents {
		flags = append(flags, git.ValueFlag{Name: "-p", Value: parent.String()})
	}

	flags = append(flags, git.ValueFlag{Name: "-F", Value: "-"})

	var stdout, stderr bytes.Buffer

	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name:  "commit-tree",
			Flags: flags,
			Args:  commitArgs,
		},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithStdin(strings.NewReader(cfg.Message)),
		git.WithEnv(env...),
	); err != nil {
		return "", fmt.Errorf("commit-tree: %w: %s %s", err, stderr.String(), stdout.String())
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("detecting object hash: %w", err)
	}

	oid, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	if cfg.Reference != "" {
		if err := repo.UpdateRef(
			ctx,
			git.ReferenceName(cfg.Reference),
			oid,
			"",
		); err != nil {
			return "", err
		}
	}

	return oid, nil
}

// WriteTestCommit writes a new commit into the target repository.
func WriteTestCommit(tb testing.TB, repo *Repo, opts ...WriteCommitOption) git.ObjectID {
	tb.Helper()

	var writeCommitConfig WriteCommitConfig

	for _, opt := range opts {
		opt(&writeCommitConfig)
	}

	if writeCommitConfig.Message == "" {
		writeCommitConfig.Message = "message"
	}

	if len(writeCommitConfig.TreeEntries) > 0 && writeCommitConfig.TreeID != "" {
		require.FailNow(tb, "cannot set tree entries and tree ID at the same time")
	}

	ctx := testhelper.Context(tb)

	var err error

	if writeCommitConfig.TreeID == "" {
		if writeCommitConfig.TreeEntries != nil {
			writeCommitConfig.TreeID = WriteTestTree(tb, repo, writeCommitConfig.TreeEntries)
		} else if len(writeCommitConfig.Parents) == 0 {
			writeCommitConfig.TreeID = WriteTestTree(tb, repo, []TreeEntry{})
		} else {
			writeCommitConfig.TreeID = git.ObjectID(writeCommitConfig.Parents[0].String() + "^{tree}")
		}
	}

	if writeCommitConfig.AuthorName == "" {
		writeCommitConfig.AuthorName = gittest.DefaultCommitterName
		writeCommitConfig.AuthorEmail = gittest.DefaultCommitterMail
	}

	if writeCommitConfig.AuthorDate.IsZero() {
		writeCommitConfig.AuthorDate = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("UTC+1", 1*60*60))
	}

	if writeCommitConfig.CommitterName == "" {
		writeCommitConfig.CommitterName = gittest.DefaultCommitterName
		writeCommitConfig.CommitterEmail = gittest.DefaultCommitterMail
	}

	if writeCommitConfig.CommitterDate.IsZero() {
		writeCommitConfig.CommitterDate = time.Date(2019, 11, 3, 11, 27, 59, 0, time.FixedZone("UTC+1", 1*60*60))
	}

	oid, err := repo.WriteCommit(ctx, writeCommitConfig)
	require.NoError(tb, err)
	return oid
}

func authorEqualIgnoringDate(tb testing.TB, expected *gitalypb.CommitAuthor, actual *gitalypb.CommitAuthor) {
	tb.Helper()
	require.Equal(tb, expected.GetName(), actual.GetName(), "author name does not match")
	require.Equal(tb, expected.GetEmail(), actual.GetEmail(), "author mail does not match")
}

// CommitEqual tests if two `GitCommit`s are equal
func CommitEqual(tb testing.TB, expected, actual *gitalypb.GitCommit) {
	tb.Helper()

	authorEqualIgnoringDate(tb, expected.GetAuthor(), actual.GetAuthor())
	authorEqualIgnoringDate(tb, expected.GetCommitter(), actual.GetCommitter())
	require.Equal(tb, expected.GetBody(), actual.GetBody(), "body does not match")
	require.Equal(tb, expected.GetSubject(), actual.GetSubject(), "subject does not match")
	require.Equal(tb, expected.GetId(), actual.GetId(), "object ID does not match")
	require.Equal(tb, expected.GetParentIds(), actual.GetParentIds(), "parent IDs do not match")
}
