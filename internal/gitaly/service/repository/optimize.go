package repository

import (
	"context"

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

	var strategyConstructor housekeeping.OptimizationStrategyConstructor
	switch in.GetStrategy() {
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_UNSPECIFIED, gitalypb.OptimizeRepositoryRequest_STRATEGY_HEURISTICAL:
		strategyConstructor = func(info stats.RepositoryInfo) housekeeping.OptimizationStrategy {
			return housekeeping.NewHeuristicalOptimizationStrategy(info)
		}
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_EAGER:
		strategyConstructor = func(info stats.RepositoryInfo) housekeeping.OptimizationStrategy {
			return housekeeping.NewEagerOptimizationStrategy(info)
		}
	default:
		return nil, structerr.NewInvalidArgument("unsupported optimization strategy %d", in.GetStrategy())
	}

	if err := s.housekeepingManager.OptimizeRepository(ctx, repo,
		housekeeping.WithOptimizationStrategyConstructor(strategyConstructor),
	); err != nil {
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
