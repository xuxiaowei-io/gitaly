package objectpool

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
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
		if errors.Is(err, objectpool.ErrInvalidPoolDir) {
			return nil, errInvalidPoolDir
		}

		return nil, helper.ErrInternalf("%w", err)
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}
