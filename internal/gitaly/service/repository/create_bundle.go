package repository

import (
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func (s *server) CreateBundle(req *gitalypb.CreateBundleRequest, stream gitalypb.RepositoryService_CreateBundleServer) error {
	ctx := stream.Context()

	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("CreateBundle: %w", err)
	}

	repo := s.localrepo(repository)

	if err := housekeeping.CleanupWorktrees(ctx, repo); err != nil {
		return structerr.NewInternal("cleaning up worktrees: %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleResponse{Data: p})
	})

	if err := repo.CreateBundle(ctx, writer, nil); err != nil {
		return err
	}

	return nil
}
