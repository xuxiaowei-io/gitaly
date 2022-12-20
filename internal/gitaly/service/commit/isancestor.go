package commit

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func validateCommitIsAncestorRequest(in *gitalypb.CommitIsAncestorRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if in.GetAncestorId() == "" {
		return errors.New("empty ancestor sha")
	}
	if in.GetChildId() == "" {
		return errors.New("empty child sha")
	}
	return nil
}

func (s *server) CommitIsAncestor(ctx context.Context, in *gitalypb.CommitIsAncestorRequest) (*gitalypb.CommitIsAncestorResponse, error) {
	if err := validateCommitIsAncestorRequest(in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
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

	cmd, err := s.gitCmdFactory.New(ctx, repo, git.Command{
		Name:  "merge-base",
		Flags: []git.Option{git.Flag{Name: "--is-ancestor"}}, Args: []string{ancestorID, childID},
	})
	if err != nil {
		return false, structerr.NewInternal("%w", err)
	}

	return cmd.Wait() == nil, nil
}
