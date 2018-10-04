package wiki

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

type server struct {
	*rubyserver.Server
}

// NewServer creates a new instance of a grpc WikiServiceServer
func NewServer(rs *rubyserver.Server) gitalypb.WikiServiceServer {
	return &server{rs}
}
