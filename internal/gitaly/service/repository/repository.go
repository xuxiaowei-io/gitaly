package repository

import (
	"context"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) RepositoryExists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	if in.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}
	path, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryExistsResponse{Exists: storage.IsGitDirectory(path)}, nil
}

func (s *server) HasLocalBranches(ctx context.Context, in *gitalypb.HasLocalBranchesRequest) (*gitalypb.HasLocalBranchesResponse, error) {
	if in.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}
	hasBranches, err := s.localrepo(in.GetRepository()).HasBranches(ctx)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.HasLocalBranchesResponse{Value: hasBranches}, nil
}
