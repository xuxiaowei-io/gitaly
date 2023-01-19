package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	repo := s.localrepo(in.GetRepository())

	repositoryInfo, err := stats.RepositoryInfoForRepository(repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}
	repositoryInfo.Log(ctx)

	var strategyOpt housekeeping.OptimizeRepositoryOption
	switch in.GetStrategy() {
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_UNSPECIFIED, gitalypb.OptimizeRepositoryRequest_STRATEGY_HEURISTICAL:
		strategy := housekeeping.NewHeuristicalOptimizationStrategy(repositoryInfo)
		strategyOpt = housekeeping.WithOptimizationStrategy(strategy)
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_EAGER:
		strategy := housekeeping.NewEagerOptimizationStrategy(repositoryInfo)
		strategyOpt = housekeeping.WithOptimizationStrategy(strategy)
	default:
		return nil, structerr.NewInvalidArgument("unsupported optimization strategy %d", in.GetStrategy())
	}

	if err := s.housekeepingManager.OptimizeRepository(ctx, repo, strategyOpt); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func (s *server) validateOptimizeRepositoryRequest(in *gitalypb.OptimizeRepositoryRequest) error {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	_, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	return nil
}
