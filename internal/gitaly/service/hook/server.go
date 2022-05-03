package hook

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedHookServiceServer
	manager          gitalyhook.Manager
	gitCmdFactory    git.CommandFactory
	packObjectsCache streamcache.Cache
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(manager gitalyhook.Manager, gitCmdFactory git.CommandFactory, packObjectsCache streamcache.Cache) gitalypb.HookServiceServer {
	srv := &server{
		manager:          manager,
		gitCmdFactory:    gitCmdFactory,
		packObjectsCache: packObjectsCache,
	}

	return srv
}
