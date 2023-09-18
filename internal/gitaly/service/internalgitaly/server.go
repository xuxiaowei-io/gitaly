package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedInternalGitalyServer
	storages []config.Storage
	locator  storage.Locator
}

// NewServer return an instance of the Gitaly service.
func NewServer(deps *service.Dependencies) gitalypb.InternalGitalyServer {
	return &server{
		storages: deps.GetCfg().Storages,
		locator:  deps.GetLocator(),
	}
}
