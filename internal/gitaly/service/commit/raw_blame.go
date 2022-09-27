package commit

import (
	"errors"
	"io"
	"regexp"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

var validBlameRange = regexp.MustCompile(`\A\d+,\d+\z`)

func (s *server) RawBlame(in *gitalypb.RawBlameRequest, stream gitalypb.CommitService_RawBlameServer) error {
	if err := validateRawBlameRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()
	revision := string(in.GetRevision())
	path := string(in.GetPath())
	blameRange := string(in.GetRange())

	flags := []git.Option{git.Flag{Name: "-p"}}
	if blameRange != "" {
		flags = append(flags, git.ValueFlag{Name: "-L", Value: blameRange})
	}

	cmd, err := s.gitCmdFactory.New(ctx, in.Repository, git.SubCmd{
		Name:        "blame",
		Flags:       flags,
		Args:        []string{revision},
		PostSepArgs: []string{path},
	})
	if err != nil {
		return helper.ErrInternalf("cmd: %w", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawBlameResponse{Data: p})
	})

	_, err = io.Copy(sw, cmd)
	if err != nil {
		return helper.ErrUnavailablef("send: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Info("ignoring git-blame error")
	}

	return nil
}

func validateRawBlameRequest(in *gitalypb.RawBlameRequest) error {
	if in.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	if err := git.ValidateRevision(in.Revision); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return errors.New("empty Path")
	}

	blameRange := in.GetRange()
	if len(blameRange) > 0 && !validBlameRange.Match(blameRange) {
		return errors.New("invalid Range")
	}

	return nil
}
