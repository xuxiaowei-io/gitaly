package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) RestoreRepository(ctx context.Context, in *gitalypb.RestoreRepositoryRequest) (*gitalypb.RestoreRepositoryResponse, error) {
	if s.backupSink == nil || s.backupLocator == nil {
		return nil, structerr.NewFailedPrecondition("restore repository: server-side backups are not configured")
	}
	if err := s.validateRestoreRepositoryRequest(in); err != nil {
		return nil, structerr.NewInvalidArgument("restore repository: %w", err)
	}

	manager := backup.NewManagerLocal(
		s.backupSink,
		s.backupLocator,
		s.locator,
		s.gitCmdFactory,
		s.catfileCache,
		s.txManager,
		in.GetBackupId(),
	)

	if err := manager.Restore(ctx, &backup.RestoreRequest{
		Repository:       in.GetRepository(),
		VanityRepository: in.GetVanityRepository(),
		AlwaysCreate:     in.GetAlwaysCreate(),
	}); err != nil {
		return nil, structerr.NewInternal("restore repository: %w", err)
	}

	return &gitalypb.RestoreRepositoryResponse{}, nil
}

func (s *server) validateRestoreRepositoryRequest(in *gitalypb.RestoreRepositoryRequest) error {
	if in.GetBackupId() == "" {
		return fmt.Errorf("empty BackupId")
	}

	if err := s.locator.ValidateRepository(in.GetRepository(),
		storage.WithSkipRepositoryExistenceCheck(),
	); err != nil {
		return fmt.Errorf("repository: %w", err)
	}

	if err := s.locator.ValidateRepository(in.GetVanityRepository(),
		storage.WithSkipStorageExistenceCheck(),
	); err != nil {
		return fmt.Errorf("vanity repository: %w", err)
	}

	return nil
}
