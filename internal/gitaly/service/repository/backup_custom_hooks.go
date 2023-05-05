package repository

import (
	"os"
	"path/filepath"
	"runtime"

	"gitlab.com/gitlab-org/gitaly/proto/v15/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

const customHooksDir = "custom_hooks"

func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	repoPath, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return structerr.NewInternal("getting repo path failed %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.BackupCustomHooksResponse{Data: p})
	})

	if _, err := os.Lstat(filepath.Join(repoPath, customHooksDir)); os.IsNotExist(err) {
		return nil
	}

	ctx := stream.Context()

	var tar []string
	if runtime.GOOS == "darwin" {
		tar = []string{"tar", "--no-mac-metadata", "-c", "-f", "-", "-C", repoPath, customHooksDir}
	} else {
		tar = []string{"tar", "-c", "-f", "-", "-C", repoPath, customHooksDir}
	}
	cmd, err := command.New(ctx, tar, command.WithStdout(writer))
	if err != nil {
		return structerr.NewInternal("%w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}
