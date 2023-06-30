package repoutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
)

// Remove will remove a repository in a race-free way with proper transactional semantics.
func Remove(
	ctx context.Context,
	locator storage.Locator,
	txManager transaction.Manager,
	repository storage.Repository,
) error {
	path, err := locator.GetRepoPath(repository, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	tempDir, err := locator.TempDir(repository.GetStorageName())
	if err != nil {
		return structerr.NewInternal("temporary directory: %w", err)
	}

	if err := os.MkdirAll(tempDir, perm.SharedDir); err != nil {
		return structerr.NewInternal("%w", err)
	}

	base := filepath.Base(path)
	destDir := filepath.Join(tempDir, base+"+removed")

	// Check whether the repository exists. If not, then there is nothing we can
	// remove. Historically, we didn't return an error in this case, which was just
	// plain bad RPC design: callers should be able to act on this, and if they don't
	// care they may still just return `NotFound` errors.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return structerr.NewNotFound("repository does not exist")
		}

		return structerr.NewInternal("statting repository: %w", err)
	}

	// Lock the repository such that it cannot be created or removed by any concurrent
	// RPC call.
	unlock, err := Lock(ctx, locator, repository)
	if err != nil {
		if errors.Is(err, safe.ErrFileAlreadyLocked) {
			return structerr.NewFailedPrecondition("repository is already locked")
		}
		return structerr.NewInternal("locking repository for removal: %w", err)
	}
	defer unlock()

	// Recheck whether the repository still exists after we have taken the lock. It
	// could be a concurrent RPC call removed the repository while we have not yet been
	// holding the lock.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return structerr.NewNotFound("repository was concurrently removed")
		}
		return structerr.NewInternal("re-statting repository: %w", err)
	}

	if err := voteOnAction(ctx, txManager, repository, voting.Prepared); err != nil {
		return structerr.NewInternal("vote on rename: %w", err)
	}

	// We move the repository into our temporary directory first before we start to
	// delete it. This is done such that we don't leave behind a partially-removed and
	// thus likely corrupt repository.
	if err := os.Rename(path, destDir); err != nil {
		return structerr.NewInternal("staging repository for removal: %w", err)
	}

	if err := safe.NewSyncer().SyncParent(path); err != nil {
		return fmt.Errorf("sync removal: %w", err)
	}

	if err := os.RemoveAll(destDir); err != nil {
		return structerr.NewInternal("removing repository: %w", err)
	}

	if err := voteOnAction(ctx, txManager, repository, voting.Committed); err != nil {
		return structerr.NewInternal("vote on finalizing: %w", err)
	}

	return nil
}

func voteOnAction(
	ctx context.Context,
	txManager transaction.Manager,
	repo storage.Repository,
	phase voting.Phase,
) error {
	return transaction.RunOnContext(ctx, func(tx txinfo.Transaction) error {
		var voteStep string
		switch phase {
		case voting.Prepared:
			voteStep = "pre-remove"
		case voting.Committed:
			voteStep = "post-remove"
		default:
			return fmt.Errorf("invalid removal step: %d", phase)
		}

		vote := fmt.Sprintf("%s %s", voteStep, repo.GetRelativePath())
		return txManager.Vote(ctx, tx, voting.VoteFromData([]byte(vote)), phase)
	})
}
