package repository

import (
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) error {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	attrFile := filepath.Join(repoPath, "info", "attributes")
	f, err := os.Open(attrFile)
	if err != nil {
		if os.IsNotExist(err) {
			return stream.Send(&gitalypb.GetInfoAttributesResponse{})
		}

		return structerr.NewInternal("failure to read info attributes: %w", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{
			Attributes: p,
		})
	})

	_, err = io.Copy(sw, f)
	return err
}
