package commit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

var (
	// ErrMissingTree indicates the tree oid has not been provided
	ErrMissingTree = errors.New("tree is missing")
	// ErrMissingAuthor indicates the author is missing
	ErrMissingAuthor = errors.New("author is missing")
	// ErrMissingCommitter indicates the committer is missing
	ErrMissingCommitter = errors.New("committer is missing")
	// ErrMissingAuthorEmail indicates the author email is missing
	ErrMissingAuthorEmail = errors.New("author email is missing")
	// ErrMissingCommitterEmail indicates the committer email is missing
	ErrMissingCommitterEmail = errors.New("committer email is missing")
)

// Config is a config for writing a new commit
type Config struct {
	Reference          string
	Parents            []git.ObjectID
	AuthorDate         time.Time
	AuthorName         string
	AuthorEmail        string
	CommitterName      string
	CommitterEmail     string
	CommitterDate      time.Time
	Message            string
	TreeID             git.ObjectID
	AlternateObjectDir string
}

// Write writes a new commit into the target repository.
func Write(ctx context.Context, repo *localrepo.Repo, cfg Config) (git.ObjectID, error) {
	var tree git.ObjectID
	var err error

	if cfg.TreeID == "" {
		return "", ErrMissingTree
	}

	tree = cfg.TreeID

	if cfg.AuthorName == "" {
		return "", ErrMissingAuthor
	}

	if cfg.CommitterName == "" {
		return "", ErrMissingCommitter
	}

	if cfg.AuthorEmail == "" {
		return "", ErrMissingAuthorEmail
	}

	if cfg.CommitterEmail == "" {
		return "", ErrMissingCommitterEmail
	}

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

	flags := []git.Option{
		git.ValueFlag{Name: "-m", Value: cfg.Message},
	}

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

	for _, parent := range cfg.Parents {
		flags = append(flags, git.ValueFlag{Name: "-p", Value: parent.String()})
	}

	var stdout, stderr bytes.Buffer

	if err := repo.ExecAndWait(ctx,
		git.Command{
			Name:  "commit-tree",
			Flags: flags,
			Args:  commitArgs,
		},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithEnv(env...),
	); err != nil {
		return "", fmt.Errorf("commit-tree: %w: %s %s", err, stderr.String(), stdout.String())
	}

	oid, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	if cfg.Reference != "" {
		if err := repo.UpdateRef(
			ctx,
			git.ReferenceName(cfg.Reference),
			oid,
			git.ObjectHashSHA1.ZeroOID,
		); err != nil {
			return "", err
		}
	}

	return oid, nil
}
