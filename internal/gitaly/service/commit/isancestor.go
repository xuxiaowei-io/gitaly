package commit

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateCommitIsAncestorRequest(in *gitalypb.CommitIsAncestorRequest) error {
	if in.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if in.GetAncestorId() == "" {
		return errors.New("Bad Request (empty ancestor sha)") //nolint:stylecheck
	}
	if in.GetChildId() == "" {
		return errors.New("Bad Request (empty child sha)") //nolint:stylecheck
	}
	return nil
}

func (s *server) CommitIsAncestor(ctx context.Context, in *gitalypb.CommitIsAncestorRequest) (*gitalypb.CommitIsAncestorResponse, error) {
	if err := validateCommitIsAncestorRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	ret, err := s.commitIsAncestorName(ctx, in.Repository, in.AncestorId, in.ChildId)
	return &gitalypb.CommitIsAncestorResponse{Value: ret}, err
}

// Assumes that `path`, `ancestorID` and `childID` are populated :trollface:
func (s *server) commitIsAncestorName(ctx context.Context, repo *gitalypb.Repository, ancestorID, childID string) (bool, error) {
	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"ancestorSha": ancestorID,
		"childSha":    childID,
	}).Debug("commitIsAncestor")

	cmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{
		Name:  "merge-base",
		Flags: []git.Option{git.Flag{Name: "--is-ancestor"}}, Args: []string{ancestorID, childID},
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return false, err
		}
		return false, status.Errorf(codes.Internal, err.Error())
	}

	return cmd.Wait() == nil, nil
}
