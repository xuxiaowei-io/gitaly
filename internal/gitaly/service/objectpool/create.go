package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	origin := s.localrepo(in.GetOrigin())

	if pool.Exists() {
		return nil, status.Errorf(codes.FailedPrecondition, "pool already exists at: %v", pool.GetRelativePath())
	}

	if err := pool.Create(ctx, origin); err != nil {
		return nil, err
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}
