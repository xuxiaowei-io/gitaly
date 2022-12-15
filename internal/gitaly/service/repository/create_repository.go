package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	hash, err := git.ObjectHashByProto(req.GetObjectFormat())
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := repoutil.Create(
		ctx,
		s.locator,
		s.gitCmdFactory,
		s.txManager,
		repository,
		func(repo *gitalypb.Repository) error {
			// We do not want to seed the repository with any contents, so we just
			// return directly.
			return nil
		},
		repoutil.WithBranchName(string(req.GetDefaultBranch())),
		repoutil.WithObjectHash(hash),
	); err != nil {
		return nil, structerr.NewInternal("creating repository: %w", err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
