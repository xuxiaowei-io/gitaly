package objectpool

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

var (
	errInvalidPoolDir = structerr.NewInvalidArgument("%w", objectpool.ErrInvalidPoolDir)

	// errMissingPool is returned when the request is missing the object pool.
	errMissingPool = structerr.NewInvalidArgument("no object pool repository")
)

// PoolRequest is the interface of a gRPC request that carries an object pool.
type PoolRequest interface {
	GetObjectPool() *gitalypb.ObjectPool
}

// ExtractPool returns the pool repository from the request or an error if the
// request did no contain a pool.
func ExtractPool(req PoolRequest) (*gitalypb.Repository, error) {
	poolRepo := req.GetObjectPool().GetRepository()
	if poolRepo == nil {
		return nil, errMissingPool
	}

	return poolRepo, nil
}

func (s *server) poolForRequest(req PoolRequest) (*objectpool.ObjectPool, error) {
	pool, err := objectpool.FromProto(s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.housekeepingManager, req.GetObjectPool())
	if err != nil {
		if err == objectpool.ErrInvalidPoolDir {
			return nil, errInvalidPoolDir
		}

		if errors.Is(err, objectpool.ErrInvalidPoolRepository) {
			return nil, structerr.NewFailedPrecondition("%w", err)
		}

		return nil, structerr.NewInternal("%w", err)
	}

	return pool, nil
}
