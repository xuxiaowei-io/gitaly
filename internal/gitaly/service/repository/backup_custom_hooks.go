package repository

import (
	"os"
	"path/filepath"
	"runtime"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const customHooksDir = "custom_hooks"

func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	repoPath, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return status.Errorf(codes.Internal, "BackupCustomHooks: getting repo path failed %v", err)
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
		return status.Errorf(codes.Internal, "%v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}
