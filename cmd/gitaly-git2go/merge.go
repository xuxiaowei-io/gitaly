//go:build static && system_libgit2

package main

import (
	"errors"
	"fmt"

	git "github.com/libgit2/git2go/v34"
)

func getConflictingFiles(index *git.Index) ([]string, error) {
	conflicts, err := getConflicts(index)
	if err != nil {
		return nil, fmt.Errorf("getting conflicts: %w", err)
	}

	conflictingFiles := make([]string, 0, len(conflicts))
	for _, conflict := range conflicts {
		switch {
		case conflict.Our != nil:
			conflictingFiles = append(conflictingFiles, conflict.Our.Path)
		case conflict.Ancestor != nil:
			conflictingFiles = append(conflictingFiles, conflict.Ancestor.Path)
		case conflict.Their != nil:
			conflictingFiles = append(conflictingFiles, conflict.Their.Path)
		default:
			return nil, errors.New("invalid conflict")
		}
	}

	return conflictingFiles, nil
}

func getConflicts(index *git.Index) ([]git.IndexConflict, error) {
	var conflicts []git.IndexConflict

	iterator, err := index.ConflictIterator()
	if err != nil {
		return nil, err
	}
	defer iterator.Free()

	for {
		conflict, err := iterator.Next()
		if err != nil {
			if git.IsErrorCode(err, git.ErrorCodeIterOver) {
				break
			}
			return nil, err
		}

		conflicts = append(conflicts, conflict)
	}

	return conflicts, nil
}
