package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) RepositoryExists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	path, err := s.locator.GetRepoPath(in.Repository, storage.WithRepositoryVerificationSkipped())
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryExistsResponse{Exists: storage.IsGitDirectory(path)}, nil
}
