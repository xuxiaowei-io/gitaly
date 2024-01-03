package repository

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) error {
	return structerr.NewUnimplemented(
		"GetInfoAttributes is deprecated in git 2.43.0+, please use GetFilesAttributes instead")
}
