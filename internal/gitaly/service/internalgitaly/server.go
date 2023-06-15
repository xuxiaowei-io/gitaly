package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedInternalGitalyServer
	storages []config.Storage
	locator  storage.Locator
}

// NewServer return an instance of the Gitaly service.
func NewServer(
	storages []config.Storage,
	locator storage.Locator,
) gitalypb.InternalGitalyServer {
	return &server{
		storages: storages,
		locator:  locator,
	}
}
