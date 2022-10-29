//go:build static && system_libgit2

package main

import (
	"fmt"

	git "github.com/libgit2/git2go/v34"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
)

func lookupCommit(repo *git.Repository, ref string) (*git.Commit, error) {
	object, err := repo.RevparseSingle(ref)
	switch {
	case git.IsErrorCode(err, git.ErrorCodeNotFound):
		return nil, git2go.CommitNotFoundError{Revision: ref}
	case err != nil:
		return nil, fmt.Errorf("lookup commit %q: %w", ref, err)
	}

	peeled, err := object.Peel(git.ObjectCommit)
	if err != nil {
		return nil, fmt.Errorf("lookup commit %q: peel: %w", ref, err)
	}

	commit, err := peeled.AsCommit()
	if err != nil {
		return nil, fmt.Errorf("lookup commit %q: as commit: %w", ref, err)
	}

	return commit, nil
}
