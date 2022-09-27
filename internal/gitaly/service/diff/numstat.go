package diff

import (
	"io"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/diff"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

var maxNumStatBatchSize = 1000

func (s *server) DiffStats(in *gitalypb.DiffStatsRequest, stream gitalypb.DiffService_DiffStatsServer) error {
	if err := s.validateDiffStatsRequestParams(in); err != nil {
		return err
	}

	var batch []*gitalypb.DiffStats
	cmd, err := s.gitCmdFactory.New(stream.Context(), in.Repository, git.SubCmd{
		Name:  "diff",
		Flags: []git.Option{git.Flag{Name: "--numstat"}, git.Flag{Name: "-z"}},
		Args:  []string{in.LeftCommitId, in.RightCommitId},
	})
	if err != nil {
		return helper.ErrInternalf("cmd: %w", err)
	}

	parser := diff.NewDiffNumStatParser(cmd)

	for {
		stat, err := parser.NextNumStat()
		if err != nil {
			if err == io.EOF {
				break
			}

			return helper.ErrInternalf("next num stat: %w", err)
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
		return helper.ErrUnavailable(err)
	}

	return sendStats(batch, stream)
}

func sendStats(batch []*gitalypb.DiffStats, stream gitalypb.DiffService_DiffStatsServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.DiffStatsResponse{Stats: batch}); err != nil {
		return helper.ErrUnavailablef("send: %w", err)
	}

	return nil
}

func (s *server) validateDiffStatsRequestParams(in *gitalypb.DiffStatsRequest) error {
	repo := in.GetRepository()
	if repo == nil {
		return helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}
	if _, err := s.locator.GetRepoPath(repo); err != nil {
		return err
	}

	if err := validateRequest(in); err != nil {
		return helper.ErrInvalidArgumentf("%w", err)
	}

	return nil
}
