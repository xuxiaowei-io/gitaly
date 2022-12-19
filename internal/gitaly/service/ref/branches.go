package ref

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) FindBranch(ctx context.Context, req *gitalypb.FindBranchRequest) (*gitalypb.FindBranchResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	if len(req.GetName()) == 0 {
		return nil, structerr.NewInvalidArgument("Branch name cannot be empty")
	}

	repo := s.localrepo(repository)

	branchName := git.NewReferenceNameFromBranchName(string(req.GetName()))
	branchRef, err := repo.GetReference(ctx, branchName)
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return &gitalypb.FindBranchResponse{}, nil
		}
		return nil, err
	}
	commit, err := repo.ReadCommit(ctx, git.Revision(branchRef.Target))
	if err != nil {
		return nil, err
	}

	branch, ok := branchName.Branch()
	if !ok {
		return nil, structerr.NewInvalidArgument("reference is not a branch")
	}

	return &gitalypb.FindBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         []byte(branch),
			TargetCommit: commit,
		},
	}, nil
}
