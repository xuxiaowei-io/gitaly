package rangediff

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

// RawRangeDiff processes the RangeDiffRequest and sends the result back to the client.
func (s *server) RawRangeDiff(in *gitalypb.RangeDiffRequest, stream gitalypb.RangeDiffService_RawRangeDiffServer) error {
	if in.GetRangeNotation() != gitalypb.RangeDiffRequest_TWO_REVS {
		return structerr.NewInvalidArgument("only TWO_REVS range notation is supported")
	}

	if !isValidRevision(stream.Context(), in.GetRev1OrRange1()) || !isValidRevision(stream.Context(), in.GetRev2OrRange2()) {
		return structerr.NewInvalidArgument("revisions are not valid")
	}

	// Create the range-diff command
	cmd := git.Command{
		Name:  "range-diff",
		Flags: []git.Option{},
		Args:  []string{in.GetRev1OrRange1(), "...", in.GetRev2OrRange2()},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawRangeDiffResponse{Data: p})
	})

	return sendRawOutput(stream.Context(), s.gitCmdFactory, in.Repository, sw, cmd)
}

// sendRawOutput runs the provided git.Command and sends the output to the provided io.Writer.
func sendRawOutput(ctx context.Context, gitCmdFactory git.CommandFactory, repo *gitalypb.Repository, sender io.Writer, subCmd git.Command) error {
	cmd, err := gitCmdFactory.New(ctx, repo, subCmd)
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	if _, err := io.Copy(sender, cmd); err != nil {
		return structerr.NewAborted("send: %w", err)
	}

	return cmd.Wait()
}

func isValidRevision(ctx context.Context, revision string) bool {
	if len(revision) == 0 {
		return false
	}
	return true
}
