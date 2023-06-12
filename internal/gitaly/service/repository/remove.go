package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) RemoveRepository(ctx context.Context, in *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository, storage.WithSkipRepositoryExistenceCheck()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Remove(ctx, s.locator, s.txManager, repository); err != nil {
		return nil, err
	}

	return &gitalypb.RemoveRepositoryResponse{}, nil
}
