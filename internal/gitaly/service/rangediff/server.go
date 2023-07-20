package rangediff

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedRangeDiffServiceServer
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
}

// NewServer creates a new instance of a grpc RangeDiffServiceServer
func NewServer(locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.RangeDiffServiceServer {
	return &server{
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
	}
}
