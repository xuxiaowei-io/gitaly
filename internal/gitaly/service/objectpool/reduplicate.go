package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) ReduplicateRepository(ctx context.Context, req *gitalypb.ReduplicateRepositoryRequest) (*gitalypb.ReduplicateRepositoryResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, helper.ErrInvalidArgumentf("%w", err)
	}

	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name: "repack",
		Flags: []git.Option{
			git.Flag{Name: "--quiet"},
			git.Flag{Name: "-a"},
			// This can be removed as soon as we have upstreamed a
			// `repack.updateServerInfo` config option. See gitlab-org/git#105 for more
			// details.
			git.Flag{Name: "-n"},
		},
	})
	if err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return &gitalypb.ReduplicateRepositoryResponse{}, nil
}
