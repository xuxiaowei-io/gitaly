package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
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

	if !stats.IsPoolRepository(poolRepo) {
		return nil, errInvalidPoolDir
	}

	if err := repoutil.Create(ctx, s.locator, s.gitCmdFactory, s.txManager, poolRepo, func(poolRepo *gitalypb.Repository) error {
		if _, err := objectpool.Create(
			ctx,
			s.locator,
			s.gitCmdFactory,
			s.catfileCache,
			s.txManager,
			s.housekeepingManager,
			&gitalypb.ObjectPool{
				Repository: poolRepo,
			},
			s.localrepo(in.GetOrigin()),
		); err != nil {
			return err
		}

		return nil
	}, repoutil.WithSkipInit()); err != nil {
		err := structerr.New("creating object pool: %w", err)

		// This is really ugly: Rails expects a FailedPrecondition error code in its
		// rspecs, but `repoutil.Create()` returns `AlreadyExists` instead. So in
		// case we see that error code we override it. We should eventually fix this
		// and instead use something like structured errors.
		if err.Code() == codes.AlreadyExists {
			err = err.WithGRPCCode(codes.FailedPrecondition)
		}

		return nil, err
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}
