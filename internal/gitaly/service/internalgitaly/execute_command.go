package internalgitaly

import (
	"bytes"
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) ExecuteCommand(
	ctx context.Context,
	req *gitalypb.ExecuteCommandRequest,
) (*gitalypb.ExecuteCommandResponse, error) {
	repo := req.GetRepository()

	if repo == nil {
		return nil, structerr.NewInvalidArgument("repository cannot be empty")
	}

	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return nil, structerr.NewInternal("error getting repo path %w", err)
	}

	var stdout, stderr bytes.Buffer

	cmd, err := command.New(
		ctx,
		s.logger,
		req.GetArgs(),
		command.WithStdout(&stdout),
		command.WithStderr(&stderr),
		command.WithDir(repoPath),
	)
	if err != nil {
		return nil, structerr.NewInternal("error creating command: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		exitCode, found := command.ExitStatus(err)
		if found {
			return &gitalypb.ExecuteCommandResponse{
				ReturnCode:  int32(exitCode),
				Output:      stdout.Bytes(),
				ErrorOutput: stderr.Bytes(),
			}, nil
		}

		return nil, structerr.NewInternal("error running command: %w", err)
	}

	return &gitalypb.ExecuteCommandResponse{
		Output:      stdout.Bytes(),
		ErrorOutput: stderr.Bytes(),
	}, nil
}
