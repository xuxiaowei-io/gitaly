package repository

import (
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func (s *server) CreateBundle(req *gitalypb.CreateBundleRequest, stream gitalypb.RepositoryService_CreateBundleServer) error {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return helper.ErrInvalidArgumentf("CreateBundle: %w", err)
	}

	ctx := stream.Context()

	if _, err := s.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: repository}); err != nil {
		return helper.ErrInternalf("running Cleanup on repository: %w", err)
	}

	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name:   "bundle",
		Action: "create",
		Flags:  []git.Option{git.OutputToStdout, git.Flag{Name: "--all"}},
	})
	if err != nil {
		return helper.ErrInternalf("cmd start failed: %w", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleResponse{Data: p})
	})

	_, err = io.Copy(writer, cmd)
	if err != nil {
		return helper.ErrInternalf("stream writer failed: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return helper.ErrInternalf("cmd wait failed: %w", err)
	}

	return nil
}
