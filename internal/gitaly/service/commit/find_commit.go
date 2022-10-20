package commit

import (
	"context"
	"errors"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func validateFindCommitRequest(in *gitalypb.FindCommitRequest) error {
	if in.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return err
	}
	return nil
}

func (s *server) FindCommit(ctx context.Context, in *gitalypb.FindCommitRequest) (*gitalypb.FindCommitResponse, error) {
	if err := validateFindCommitRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	repo := s.localrepo(in.GetRepository())

	var opts []localrepo.ReadCommitOpt
	if in.GetTrailers() {
		opts = []localrepo.ReadCommitOpt{localrepo.WithTrailers()}
	}

	commit, err := repo.ReadCommit(ctx, git.Revision(in.GetRevision()), opts...)
	if err != nil {
		if errors.Is(err, localrepo.ErrObjectNotFound) {
			return &gitalypb.FindCommitResponse{}, nil
		}
		return &gitalypb.FindCommitResponse{}, err
	}

	return &gitalypb.FindCommitResponse{Commit: commit}, nil
}
