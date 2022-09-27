package repository

import (
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

const customHooksDir = "custom_hooks"

func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	if in.GetRepository() == nil {
		return helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}
	repoPath, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return helper.ErrInternalf("getting repo path failed %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.BackupCustomHooksResponse{Data: p})
	})

	if _, err := os.Lstat(filepath.Join(repoPath, customHooksDir)); os.IsNotExist(err) {
		return nil
	}

	ctx := stream.Context()
	tar := []string{"tar", "-c", "-f", "-", "-C", repoPath, customHooksDir}
	cmd, err := command.New(ctx, tar, command.WithStdout(writer))
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
