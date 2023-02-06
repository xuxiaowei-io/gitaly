package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

// ErrMissingTree indicates a missing tree when attemping to write a commit
var ErrMissingTree = errors.New("missing tree")

// ErrMissingCommitterName indicates an attempt to write a commit without a
// comitter name
var ErrMissingCommitterName = errors.New("missing committer name")

// ErrMissingAuthorName indicates an attempt to write a commit without a
// comitter name
var ErrMissingAuthorName = errors.New("missing author name")

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

func validateWriteCommitConfig(cfg WriteCommitConfig) error {
	if cfg.TreeID == "" {
		return ErrMissingTree
	}

	if cfg.AuthorName == "" {
		return ErrMissingAuthorName
	}

	if cfg.CommitterName == "" {
		return ErrMissingCommitterName
	}

	return nil
}

// WriteCommit writes a new commit into the target repository.
func (repo *Repo) WriteCommit(ctx context.Context, cfg WriteCommitConfig) (git.ObjectID, error) {
	if err := validateWriteCommitConfig(cfg); err != nil {
		return "", err
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
	commitArgs := []string{string(cfg.TreeID)}

	repoPath, err := repo.Path()
	if err != nil {
		return "", fmt.Errorf("getting repo path: %w", err)
	}

	var env []string
	if cfg.AlternateObjectDir != "" {
		if !filepath.IsAbs(cfg.AlternateObjectDir) {
			return "", errors.New("alternate object directory must be an absolute path")
		}

		if err := os.MkdirAll(cfg.AlternateObjectDir, perm.SharedDir); err != nil {
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
		return "", fmt.Errorf("hex to object hash: %w", err)
	}

	if cfg.Reference != "" {
		if err := repo.UpdateRef(
			ctx,
			git.ReferenceName(cfg.Reference),
			oid,
			"",
		); err != nil {
			return "", fmt.Errorf("updating ref: %w", err)
		}
	}

	return oid, nil
}
