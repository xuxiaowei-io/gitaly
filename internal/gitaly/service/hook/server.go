package hook

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v15/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedHookServiceServer
	manager            gitalyhook.Manager
	gitCmdFactory      git.CommandFactory
	packObjectsCache   streamcache.Cache
	concurrencyTracker *gitalyhook.ConcurrencyTracker
	packObjectsLimiter limithandler.Limiter
	runPackObjectsFn   func(
		context.Context,
		git.CommandFactory,
		io.Writer,
		*gitalypb.PackObjectsHookWithSidechannelRequest,
		*packObjectsArgs,
		io.Reader,
		string,
		*gitalyhook.ConcurrencyTracker,
	) error
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(
	manager gitalyhook.Manager,
	gitCmdFactory git.CommandFactory,
	packObjectsCache streamcache.Cache,
	concurrencyTracker *gitalyhook.ConcurrencyTracker,
	packObjectsLimiter limithandler.Limiter,
) gitalypb.HookServiceServer {
	srv := &server{
		manager:            manager,
		gitCmdFactory:      gitCmdFactory,
		packObjectsCache:   packObjectsCache,
		packObjectsLimiter: packObjectsLimiter,
		concurrencyTracker: concurrencyTracker,
		runPackObjectsFn:   runPackObjects,
	}

	return srv
}
