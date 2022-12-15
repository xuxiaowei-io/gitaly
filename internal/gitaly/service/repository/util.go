package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) removeOriginInRepo(ctx context.Context, repository *gitalypb.Repository) error {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.Command{Name: "remote", Args: []string{"remove", "origin"}}, git.WithRefTxHook(repository))
	if err != nil {
		return fmt.Errorf("remote cmd start: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("remote cmd wait: %w", err)
	}

	return nil
}
