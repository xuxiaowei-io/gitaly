package ref

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) PackRefs(ctx context.Context, in *gitalypb.PackRefsRequest) (*gitalypb.PackRefsResponse, error) {
	if err := validatePackRefsRequest(in); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	if err := s.packRefs(ctx, in.GetRepository()); err != nil {
		return nil, structerr.NewInternal("%w", err)
	}

	return &gitalypb.PackRefsResponse{}, nil
}

func validatePackRefsRequest(in *gitalypb.PackRefsRequest) error {
	return service.ValidateRepository(in.GetRepository())
}

func (s *server) packRefs(ctx context.Context, repository repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.Command{
		Name:  "pack-refs",
		Flags: []git.Option{git.Flag{Name: "--all"}},
	})
	if err != nil {
		return err
	}

	return cmd.Wait()
}
