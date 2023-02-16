package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
)

type mergeTreeConfig struct {
	allowUnrelatedHistories bool
}

// MergeTreeOption is a function that sets a config in mergeTreeConfig.
type MergeTreeOption func(*mergeTreeConfig)

// WithAllowUnrelatedHistories lets MergeTree accept two commits that do not
// share a common ancestor.
func WithAllowUnrelatedHistories() MergeTreeOption {
	return func(options *mergeTreeConfig) {
		options.allowUnrelatedHistories = true
	}
}

// MergeTree calls git-merge-tree(1) with arguments, and parses the results from
// stdout.
func (repo *Repo) MergeTree(
	ctx context.Context,
	ours, theirs string,
	mergeTreeOptions ...MergeTreeOption,
) (git.ObjectID, error) {
	var config mergeTreeConfig

	for _, option := range mergeTreeOptions {
		option(&config)
	}

	flags := []git.Option{
		git.Flag{Name: "--write-tree"},
		git.Flag{Name: "--name-only"},
	}

	if config.allowUnrelatedHistories {
		flags = append(flags, git.Flag{Name: "--allow-unrelated-histories"})
	}

	var stdout, stderr bytes.Buffer
	err := repo.ExecAndWait(
		ctx,
		git.Command{
			Name:  "merge-tree",
			Flags: flags,
			Args:  []string{ours, theirs},
		},
		git.WithStderr(&stderr),
		git.WithStdout(&stdout),
	)
	if err != nil {
		exitCode, success := command.ExitStatus(err)
		if !success {
			return "", errors.New("could not parse exit status of merge-tree(1)")
		}

		if exitCode > 1 {
			if text.ChompBytes(stderr.Bytes()) == "fatal: refusing to merge unrelated histories" {
				return "", &MergeTreeError{
					InfoMessage: "unrelated histories",
				}
			}
			return "", fmt.Errorf("merge-tree: %w", err)
		}

		return "", parseMergeTreeError(stdout.String())
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return "", fmt.Errorf("getting object hash %w", err)
	}

	oid, err := objectHash.FromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", fmt.Errorf("hex to oid: %w", err)
	}

	return oid, nil
}

// parseMergeTreeError parses the output from git-merge-tree(1)'s stdout into
// a MergeTreeResult struct. The format for the output can be found at
// https://git-scm.com/docs/git-merge-tree#OUTPUT.
func parseMergeTreeError(output string) error {
	var mergeTreeError MergeTreeError

	lines := strings.SplitN(output, "\n\n", 2)

	// When the output is of unexpected length
	if len(lines) < 2 {
		return errors.New("error parsing merge tree result")
	}

	mergeTreeError.InfoMessage = strings.TrimSuffix(lines[1], "\n")
	oidAndConflicts := strings.Split(lines[0], "\n")

	if len(oidAndConflicts) < 2 {
		return &mergeTreeError
	}

	mergeTreeError.ConflictingFiles = oidAndConflicts[1:]

	return &mergeTreeError
}

// MergeTreeError encapsulates any conflicting files and messages that occur
// when a merge-tree(1) command fails.
type MergeTreeError struct {
	ConflictingFiles []string
	InfoMessage      string
}

// Error returns the error string for a conflict error.
func (c *MergeTreeError) Error() string {
	// TODO: for now, it's better that this error matches the git2go
	// error but once we deprecate the git2go code path in
	// merges, we can change this error to print out the conflicting files
	// and the InfoMessage.
	return "merge: there are conflicting files"
}
