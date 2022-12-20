package merge

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

// revive:disable:exported // Some names in this package stutter because of the
// merge package, but this is because of the name of the git builtin
// git-merge-tree(1)

// MergeTree calls git-merge-tree(1) with arguments, and parses the results from
// stdout. If there is an error, it will return a MergeTreeError
func MergeTree(
	ctx context.Context,
	repo *localrepo.Repo,
	branch1, branch2 string,
) (git.ObjectID, error) {
	var stdout, stderr bytes.Buffer
	err := repo.ExecAndWait(
		ctx,
		git.Command{
			Name: "merge-tree",
			Flags: []git.Option{
				git.Flag{Name: "--write-tree"},
				git.Flag{Name: "--name-only"},
			},
			Args: []string{branch1, branch2},
		},
		git.WithStderr(&stderr),
		git.WithStdout(&stdout),
	)
	if exitCode, success := command.ExitStatus(err); success && exitCode > 1 {
		return "", fmt.Errorf("merge-tree: %w", err)
	}

	mergeTreeResult, err := parseResult(stdout.String())
	if err != nil {
		return "", err
	}

	return mergeTreeResult, nil
}

// parseResult parses the output from git-merge-tree(1)'s stdout into
// a MergeTreeResult struct
// The format for the output can be found at https://git-scm.com/docs/git-merge-tree#OUTPUT
func parseResult(output string) (git.ObjectID, error) {
	var mergeTreeError MergeTreeError

	lines := strings.SplitN(output, "\n\n", 2)

	if len(lines) < 2 {
		oid, err := git.ObjectHashSHA1.FromHex(strings.TrimSuffix(lines[0], "\n"))
		if err != nil {
			return "", err
		}
		return oid, nil
	}

	mergeTreeError.InfoMessage = strings.TrimSuffix(lines[1], "\n")
	oidAndConflicts := strings.Split(lines[0], "\n")
	oid, err := git.ObjectHashSHA1.FromHex(oidAndConflicts[0])
	if err != nil {
		return "", err
	}

	if len(oidAndConflicts) < 2 {
		return oid, nil
	}

	mergeTreeError.ConflictingFiles = oidAndConflicts[1:]

	return "", &mergeTreeError
}

type MergeTreeError struct {
	ConflictingFiles []string
	InfoMessage      string
}

// Error returns the error string for a conflict error
func (c *MergeTreeError) Error() string {
	// TODO: for now, it's better that this error matches the git2go
	// error but once we deprecate the git2go code path in
	// merges, we can change this error to print out the conflicting files
	// and the InfoMessage.
	return "merge: there are conflicting files"
}
