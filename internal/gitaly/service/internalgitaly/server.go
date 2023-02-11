package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

type server struct {
	gitalypb.UnimplementedInternalGitalyServer
	storages []config.Storage
}

// NewServer return an instance of the Gitaly service.
func NewServer(storages []config.Storage) gitalypb.InternalGitalyServer {
	return &server{storages: storages}
}
