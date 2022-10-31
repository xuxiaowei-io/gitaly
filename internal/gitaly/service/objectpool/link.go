package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) LinkRepositoryToObjectPool(ctx context.Context, req *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	pool, err := s.poolForRequest(req)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(repository)

	if err := pool.Link(ctx, repo); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.LinkRepositoryToObjectPoolResponse{}, nil
}
