package repository

import (
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

// GetConfig reads the repository's gitconfig file and returns its contents.
func (s *server) GetConfig(
	request *gitalypb.GetConfigRequest,
	stream gitalypb.RepositoryService_GetConfigServer,
) error {
	repository := request.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	repoPath, err := s.locator.GetPath(repository)
	if err != nil {
		return err
	}

	configPath := filepath.Join(repoPath, "config")

	gitconfig, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return helper.ErrNotFoundf("opening gitconfig: %w", err)
		}
		return helper.ErrInternalf("opening gitconfig: %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetConfigResponse{
			Data: p,
		})
	})

	if _, err := io.Copy(writer, gitconfig); err != nil {
		return helper.ErrInternalf("sending config: %w", err)
	}

	return nil
}
