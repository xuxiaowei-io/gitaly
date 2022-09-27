package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// errMissingOriginRepository is returned when the request is missing the
// origin repository.
var errMissingOriginRepository = helper.ErrInvalidArgumentf("no origin repository")

func (s *server) CreateObjectPool(ctx context.Context, in *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error) {
	if in.GetOrigin() == nil {
		return nil, errMissingOriginRepository
	}

	pool, err := s.poolForRequest(in)
	if err != nil {
		return nil, err
	}

	if pool.Exists() {
		return nil, helper.ErrFailedPreconditionf("pool already exists at: %v", pool.GetRelativePath())
	}

	origin := s.localrepo(in.GetOrigin())
	if err := pool.Create(ctx, origin); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}
