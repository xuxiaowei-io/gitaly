package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

// WriteCommitGraph write or update commit-graph file in a repository
func (s *server) WriteCommitGraph(
	ctx context.Context,
	in *gitalypb.WriteCommitGraphRequest,
) (*gitalypb.WriteCommitGraphResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localrepo(repository)

	if in.GetSplitStrategy() != gitalypb.WriteCommitGraphRequest_SizeMultiple {
		return nil, structerr.NewInvalidArgument("unsupported split strategy: %v", in.GetSplitStrategy())
	}

	writeCommitGraphCfg, err := housekeeping.WriteCommitGraphConfigForRepository(ctx, repo)
	if err != nil {
		return nil, structerr.NewInternal("getting commit-graph config: %w", err)
	}

	if err := housekeeping.WriteCommitGraph(ctx, repo, writeCommitGraphCfg); err != nil {
		return nil, err
	}

	return &gitalypb.WriteCommitGraphResponse{}, nil
}
