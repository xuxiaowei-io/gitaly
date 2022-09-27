package repository

import (
	"io"
	"os"
	"path/filepath"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) error {
	if in.GetRepository() == nil {
		return helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}
	repoPath, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	attrFile := filepath.Join(repoPath, "info", "attributes")
	f, err := os.Open(attrFile)
	if err != nil {
		if os.IsNotExist(err) {
			return stream.Send(&gitalypb.GetInfoAttributesResponse{})
		}

		return helper.ErrInternalf("failure to read info attributes: %w", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{
			Attributes: p,
		})
	})

	if _, err = io.Copy(sw, f); err != nil {
		return helper.ErrInternalf("send attributes: %w", err)
	}
	return nil
}
