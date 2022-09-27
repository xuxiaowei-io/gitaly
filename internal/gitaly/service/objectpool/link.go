package objectpool

import (
	"context"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) LinkRepositoryToObjectPool(ctx context.Context, req *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error) {
	if req.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}

	pool, err := s.poolForRequest(req)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(req.GetRepository())

	if err := pool.Link(ctx, repo); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.LinkRepositoryToObjectPoolResponse{}, nil
}
