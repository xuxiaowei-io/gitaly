package diff

import (
	"io"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

var maxNumStatBatchSize = 1000

func (s *server) DiffStats(in *gitalypb.DiffStatsRequest, stream gitalypb.DiffService_DiffStatsServer) error {
	if err := s.validateDiffStatsRequestParams(in); err != nil {
		return err
	}

	var batch []*gitalypb.DiffStats
	cmd, err := s.gitCmdFactory.New(stream.Context(), in.Repository, git.Command{
		Name:  "diff",
		Flags: []git.Option{git.Flag{Name: "--numstat"}, git.Flag{Name: "-z"}},
		Args:  []string{in.LeftCommitId, in.RightCommitId},
	})
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	parser := diff.NewDiffNumStatParser(cmd)

	for {
		stat, err := parser.NextNumStat()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		numStat := &gitalypb.DiffStats{
			Additions: stat.Additions,
			Deletions: stat.Deletions,
			Path:      stat.Path,
			OldPath:   stat.OldPath,
		}

		batch = append(batch, numStat)

		if len(batch) == maxNumStatBatchSize {
			if err := sendStats(batch, stream); err != nil {
				return err
			}

			batch = nil
		}
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewUnavailable("%w", err)
	}

	return sendStats(batch, stream)
}

func sendStats(batch []*gitalypb.DiffStats, stream gitalypb.DiffService_DiffStatsServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.DiffStatsResponse{Stats: batch}); err != nil {
		return structerr.NewUnavailable("send: %w", err)
	}

	return nil
}

func (s *server) validateDiffStatsRequestParams(in *gitalypb.DiffStatsRequest) error {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	if _, err := s.locator.GetRepoPath(repository); err != nil {
		return err
	}

	if err := validateRequest(in); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	return nil
}
