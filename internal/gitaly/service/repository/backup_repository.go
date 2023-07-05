package repository

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) BackupRepository(ctx context.Context, in *gitalypb.BackupRepositoryRequest) (*gitalypb.BackupRepositoryResponse, error) {
	if s.backupSink == nil || s.backupLocator == nil {
		return nil, structerr.NewFailedPrecondition("backup repository: server-side backups are not configured")
	}
	if err := s.validateBackupRepositoryRequest(in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	manager := backup.NewManagerLocal(
		s.backupSink,
		s.backupLocator,
		s.locator,
		s.gitCmdFactory,
		s.catfileCache,
		s.txManager,
	)

	err := manager.Create(ctx, &backup.CreateRequest{
		Repository:       in.Repository,
		VanityRepository: in.VanityRepository,
		BackupID:         in.BackupId,
	})

	switch {
	case errors.Is(err, backup.ErrSkipped):
		return nil, structerr.NewFailedPrecondition("backup repository: %w", err).WithDetail(
			&gitalypb.BackupRepositoryResponse_SkippedError{},
		)
	case err != nil:
		return nil, structerr.NewInternal("backup repository: %w", err)
	}

	return &gitalypb.BackupRepositoryResponse{}, nil
}

func (s *server) validateBackupRepositoryRequest(in *gitalypb.BackupRepositoryRequest) error {
	if in.GetBackupId() == "" {
		return fmt.Errorf("empty BackupId")
	}
	if err := s.locator.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if err := s.locator.ValidateRepository(in.GetVanityRepository(),
		storage.WithSkipStorageExistenceCheck(),
	); err != nil {
		return err
	}
	return nil
}
