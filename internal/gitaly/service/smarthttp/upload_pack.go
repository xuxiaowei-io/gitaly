package smarthttp

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) PostUploadPackWithSidechannel(ctx context.Context, req *gitalypb.PostUploadPackWithSidechannelRequest) (*gitalypb.PostUploadPackWithSidechannelResponse, error) {
	repoPath, gitConfig, err := s.validateUploadPackRequest(ctx, req)
	if err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	conn, err := sidechannel.OpenSidechannel(ctx)
	if err != nil {
		return nil, structerr.NewInternal("open sidechannel: %w", err)
	}
	defer conn.Close()

	if err := s.runUploadPack(ctx, req, repoPath, gitConfig, conn, conn); err != nil {
		return nil, structerr.NewInternal("running upload-pack: %w", err)
	}

	if err := conn.Close(); err != nil {
		return nil, structerr.NewInternal("close sidechannel connection: %w", err)
	}

	return &gitalypb.PostUploadPackWithSidechannelResponse{}, nil
}

type statsCollector struct {
	c       io.Closer
	statsCh chan stats.PackfileNegotiation
}

func (sc *statsCollector) finish() stats.PackfileNegotiation {
	sc.c.Close()
	return <-sc.statsCh
}

func (s *server) runStatsCollector(ctx context.Context, r io.Reader) (io.Reader, *statsCollector) {
	pr, pw := io.Pipe()
	sc := &statsCollector{
		c:       pw,
		statsCh: make(chan stats.PackfileNegotiation, 1),
	}

	go func() {
		defer close(sc.statsCh)

		stats, err := stats.ParsePackfileNegotiation(pr)
		if err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Debug("failed parsing packfile negotiation")
			return
		}
		stats.UpdateMetrics(s.packfileNegotiationMetrics)

		sc.statsCh <- stats
	}()

	return io.TeeReader(r, pw), sc
}

func (s *server) validateUploadPackRequest(ctx context.Context, req *gitalypb.PostUploadPackWithSidechannelRequest) (string, []git.ConfigPair, error) {
	repository := req.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return "", nil, err
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return "", nil, err
	}

	git.WarnIfTooManyBitmaps(ctx, s.locator, repository.GetStorageName(), repoPath)

	config, err := git.ConvertConfigOptions(req.GetGitConfigOptions())
	if err != nil {
		return "", nil, err
	}

	return repoPath, config, nil
}

func (s *server) runUploadPack(ctx context.Context, req *gitalypb.PostUploadPackWithSidechannelRequest, repoPath string, gitConfig []git.ConfigPair, stdin io.Reader, stdout io.Writer) error {
	h := sha1.New()

	stdin = io.TeeReader(stdin, h)
	stdin, collector := s.runStatsCollector(ctx, stdin)
	defer collector.finish()

	commandOpts := []git.CmdOpt{
		git.WithStdin(stdin),
		git.WithGitProtocol(req),
		git.WithConfig(gitConfig...),
		git.WithPackObjectsHookEnv(req.GetRepository(), "http"),
	}

	cmd, err := s.gitCmdFactory.New(ctx, req.GetRepository(), git.Command{
		Name:  "upload-pack",
		Flags: []git.Option{git.Flag{Name: "--stateless-rpc"}},
		Args:  []string{repoPath},
	}, commandOpts...)
	if err != nil {
		return structerr.NewUnavailable("spawning upload-pack: %w", err)
	}

	// Use a custom buffer size to minimize the number of system calls.
	respBytes, err := io.CopyBuffer(stdout, cmd, make([]byte, 64*1024))
	if err != nil {
		return structerr.NewUnavailable("copying stdout from upload-pack: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		stats := collector.finish()

		if _, ok := command.ExitStatus(err); ok && stats.Deepen != "" {
			// We have seen a 'deepen' message in the request. It is expected that
			// git-upload-pack has a non-zero exit status: don't treat this as an
			// error.
			return nil
		}

		return structerr.NewUnavailable("waiting for upload-pack: %w", err)
	}

	ctxlogrus.Extract(ctx).WithField("request_sha", fmt.Sprintf("%x", h.Sum(nil))).WithField("response_bytes", respBytes).Info("request details")

	return nil
}
