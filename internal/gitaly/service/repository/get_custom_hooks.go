package repository

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

const customHooksDir = "custom_hooks"

// GetCustomHooks fetches the git hooks for a repository. The hooks are sent in
// a tar archive containing a `custom_hooks` directory. If no hooks are present
// in the repository, the response will have no data.
func (s *server) GetCustomHooks(in *gitalypb.GetCustomHooksRequest, stream gitalypb.RepositoryService_GetCustomHooksServer) error {
	ctx := stream.Context()

	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("validating repository: %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetCustomHooksResponse{Data: p})
	})

	if err := s.getCustomHooks(ctx, writer, in.Repository); err != nil {
		return structerr.NewInternal("reading custom hooks: %w", err)
	}

	return nil
}

// BackupCustomHooks fetches the git hooks for a repository. The hooks are sent
// in a tar archive containing a `custom_hooks` directory. If no hooks are
// present in the repository, the response will have no data.
func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	ctx := stream.Context()

	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return structerr.NewInvalidArgument("validating repository: %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.BackupCustomHooksResponse{Data: p})
	})

	if err := s.getCustomHooks(ctx, writer, in.Repository); err != nil {
		return structerr.NewInternal("reading custom hooks: %w", err)
	}

	return nil
}

func (s *server) getCustomHooks(ctx context.Context, writer io.Writer, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	if _, err := os.Lstat(filepath.Join(repoPath, customHooksDir)); os.IsNotExist(err) {
		return nil
	}

	if err := archive.WriteTarball(ctx, writer, repoPath, customHooksDir); err != nil {
		return structerr.NewInternal("archiving hooks: %w", err)
	}

	return nil
}
