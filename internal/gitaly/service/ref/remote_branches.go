package ref

import (
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) FindAllRemoteBranches(req *gitalypb.FindAllRemoteBranchesRequest, stream gitalypb.RefService_FindAllRemoteBranchesServer) error {
	if err := validateFindAllRemoteBranchesRequest(req); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	if err := s.findAllRemoteBranches(req, stream); err != nil {
		return structerr.NewInternal("%w", err)
	}

	return nil
}

func (s *server) findAllRemoteBranches(req *gitalypb.FindAllRemoteBranchesRequest, stream gitalypb.RefService_FindAllRemoteBranchesServer) error {
	repo := s.localrepo(req.GetRepository())

	args := []git.Option{
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
	}

	patterns := []string{"refs/remotes/" + req.GetRemoteName()}

	ctx := stream.Context()
	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return fmt.Errorf("creating object reader: %w", err)
	}
	defer cancel()

	opts := buildFindRefsOpts(ctx, nil)
	opts.cmdArgs = args
	writer := newFindAllRemoteBranchesWriter(stream, objectReader)

	if err = s.findRefs(ctx, writer, repo, patterns, opts); err != nil {
		return fmt.Errorf("finding refs: %w", err)
	}

	return nil
}

func validateFindAllRemoteBranchesRequest(req *gitalypb.FindAllRemoteBranchesRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetRemoteName()) == 0 {
		return errors.New("empty RemoteName")
	}

	return nil
}
