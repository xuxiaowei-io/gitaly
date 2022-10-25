package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) DeleteObjectPool(ctx context.Context, in *gitalypb.DeleteObjectPoolRequest) (*gitalypb.DeleteObjectPoolResponse, error) {
	pool, err := s.poolForRequest(in)
	if err != nil {
		return nil, err
	}

	if err := pool.Remove(ctx); err != nil {
		return nil, err
	}

	return &gitalypb.DeleteObjectPoolResponse{}, nil
}
