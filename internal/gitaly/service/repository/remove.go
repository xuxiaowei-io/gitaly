package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) RemoveRepository(ctx context.Context, in *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	path, err := s.locator.GetPath(repository)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	tempDir, err := s.locator.TempDir(repository.GetStorageName())
	if err != nil {
		return nil, structerr.NewInternal("temporary directory: %w", err)
	}

	if err := os.MkdirAll(tempDir, perm.SharedDir); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	base := filepath.Base(path)
	destDir := filepath.Join(tempDir, base+"+removed")

	// Check whether the repository exists. If not, then there is nothing we can
	// remove. Historically, we didn't return an error in this case, which was just
	// plain bad RPC design: callers should be able to act on this, and if they don't
	// care they may still just return `NotFound` errors.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, structerr.NewNotFound("repository does not exist")
		}

		return nil, structerr.NewInternal("statting repository: %w", err)
	}

	// Lock the repository such that it cannot be created or removed by any concurrent
	// RPC call.
	unlock, err := repoutil.Lock(ctx, s.locator, repository)
	if err != nil {
		if errors.Is(err, safe.ErrFileAlreadyLocked) {
			return nil, structerr.NewFailedPrecondition("repository is already locked")
		}
		return nil, structerr.NewInternal("locking repository for removal: %w", err)
	}
	defer unlock()

	// Recheck whether the repository still exists after we have taken the lock. It
	// could be a concurrent RPC call removed the repository while we have not yet been
	// holding the lock.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, structerr.NewNotFound("repository was concurrently removed")
		}
		return nil, structerr.NewInternal("re-statting repository: %w", err)
	}

	if err := s.voteOnAction(ctx, repository, voting.Prepared); err != nil {
		return nil, structerr.NewInternal("vote on rename: %w", err)
	}

	// We move the repository into our temporary directory first before we start to
	// delete it. This is done such that we don't leave behind a partially-removed and
	// thus likely corrupt repository.
	if err := os.Rename(path, destDir); err != nil {
		return nil, structerr.NewInternal("staging repository for removal: %w", err)
	}

	if err := os.RemoveAll(destDir); err != nil {
		return nil, structerr.NewInternal("removing repository: %w", err)
	}

	if err := s.voteOnAction(ctx, repository, voting.Committed); err != nil {
		return nil, structerr.NewInternal("vote on finalizing: %w", err)
	}

	return &gitalypb.RemoveRepositoryResponse{}, nil
}

func (s *server) voteOnAction(ctx context.Context, repo *gitalypb.Repository, phase voting.Phase) error {
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
		return s.txManager.Vote(ctx, tx, voting.VoteFromData([]byte(vote)), phase)
	})
}
