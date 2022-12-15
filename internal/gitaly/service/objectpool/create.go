package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// errMissingOriginRepository is returned when the request is missing the
// origin repository.
var errMissingOriginRepository = structerr.NewInvalidArgument("no origin repository")

func (s *server) CreateObjectPool(ctx context.Context, in *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error) {
	if in.GetOrigin() == nil {
		return nil, errMissingOriginRepository
	}

	poolRepo := in.GetObjectPool().GetRepository()
	if poolRepo == nil {
		return nil, errMissingPool
	}

	if !housekeeping.IsPoolRepository(poolRepo) {
		return nil, errInvalidPoolDir
	}

	if _, err := objectpool.Create(
		ctx,
		s.locator,
		s.gitCmdFactory,
		s.catfileCache,
		s.txManager,
		s.housekeepingManager,
		in.GetObjectPool(),
		s.localrepo(in.GetOrigin()),
	); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}
