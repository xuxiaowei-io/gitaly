package objectpool

import (
	"context"
	"errors"
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
)

func (s *server) DeleteObjectPool(ctx context.Context, in *gitalypb.DeleteObjectPoolRequest) (*gitalypb.DeleteObjectPoolResponse, error) {
	pool, err := s.poolForRequest(in)
	if err != nil {
		if errors.Is(err, objectpool.ErrInvalidPoolRepository) {
			// TODO: we really should return an error in case we're trying to delete an
			// object pool that does not exist.
			return &gitalypb.DeleteObjectPoolResponse{}, nil
		}

		return nil, err
	}

	if err := pool.Remove(ctx); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.DeleteObjectPoolResponse{}, nil
}
