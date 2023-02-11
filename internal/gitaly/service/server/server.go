package server

import (
	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
)

type server struct {
	gitalypb.UnimplementedServerServiceServer
	gitCmdFactory git.CommandFactory
	storages      []config.Storage
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(gitCmdFactory git.CommandFactory, storages []config.Storage) gitalypb.ServerServiceServer {
	return &server{gitCmdFactory: gitCmdFactory, storages: storages}
}
