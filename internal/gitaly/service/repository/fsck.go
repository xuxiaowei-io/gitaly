package repository

import (
	"bytes"
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) Fsck(ctx context.Context, req *gitalypb.FsckRequest) (*gitalypb.FsckResponse, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, helper.ErrInvalidArgumentf("%w", err)
	}

	var stdout, stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.New(ctx, repository,
		git.Command{Name: "fsck"},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return &gitalypb.FsckResponse{Error: append(stdout.Bytes(), stderr.Bytes()...)}, nil
	}

	return &gitalypb.FsckResponse{}, nil
}
