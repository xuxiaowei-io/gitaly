package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	repo := s.localrepo(in.GetRepository())

	var strategyOpt housekeeping.OptimizeRepositoryOption
	switch in.GetStrategy() {
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_UNSPECIFIED, gitalypb.OptimizeRepositoryRequest_STRATEGY_HEURISTICAL:
		strategy, err := housekeeping.NewHeuristicalOptimizationStrategy(ctx, repo)
		if err != nil {
			return nil, structerr.NewInternal("creating heuristical optimization strategy: %w", err)
		}

		strategyOpt = housekeeping.WithOptimizationStrategy(strategy)
	case gitalypb.OptimizeRepositoryRequest_STRATEGY_EAGER:
		strategy, err := housekeeping.NewEagerOptimizationStrategy(ctx, repo)
		if err != nil {
			return nil, structerr.NewInternal("creating eager optimization strategy: %w", err)
		}

		strategyOpt = housekeeping.WithOptimizationStrategy(strategy)
	default:
		return nil, helper.ErrInvalidArgumentf("unsupported optimization strategy %d", in.GetStrategy())
	}

	if err := s.housekeepingManager.OptimizeRepository(ctx, repo, strategyOpt); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func (s *server) validateOptimizeRepositoryRequest(in *gitalypb.OptimizeRepositoryRequest) error {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return helper.ErrInvalidArgumentf("%w", err)
	}

	_, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	return nil
}
