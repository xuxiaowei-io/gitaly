package remote

import (
	"bytes"
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) FindRemoteRepository(ctx context.Context, req *gitalypb.FindRemoteRepositoryRequest) (*gitalypb.FindRemoteRepositoryResponse, error) {
	if req.GetRemote() == "" {
		return nil, helper.ErrInvalidArgumentf("empty remote can't be checked.")
	}

	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.Command{
			Name: "ls-remote",
			Args: []string{
				req.GetRemote(),
				"HEAD",
			},
		},
	)
	if err != nil {
		return nil, helper.ErrInternalf("error executing git command: %w", err)
	}

	output, err := io.ReadAll(cmd)
	if err != nil {
		return nil, helper.ErrInternalf("unable to read stdout: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// The output of a successful command is structured like
	// Regexp would've read better, but this is faster
	// 58fbff2e0d3b620f591a748c158799ead87b51cd	HEAD
	fields := bytes.Fields(output)
	match := len(fields) == 2 && len(fields[0]) == 40 && string(fields[1]) == "HEAD"

	return &gitalypb.FindRemoteRepositoryResponse{Exists: match}, nil
}
