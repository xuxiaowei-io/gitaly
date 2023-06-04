package smarthttp

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/protobuf/proto"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type postUploadPackRequest interface {
	GetRepository() *gitalypb.Repository
	GetGitConfigOptions() []string
	GetGitProtocol() string
}

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

	proxy := func(cmd *command.Command) (int64, error) {
		return io.CopyBuffer(conn, cmd, make([]byte, 64*1024))
	}
	if err := s.runUploadPack(ctx, req, repoPath, gitConfig, conn, proxy); err != nil {
		return nil, structerr.NewInternal("running upload-pack: %w", err)
	}
	if err := conn.Close(); err != nil {
		return nil, structerr.NewInternal("close sidechannel connection: %w", err)
	}

	return &gitalypb.PostUploadPackWithSidechannelResponse{}, nil
}

func (s *server) PostUploadPackV3(stream gitalypb.SmartHTTPService_PostUploadPackV3Server) error {
	ctx := stream.Context()

	var req gitalypb.PostUploadPackV3Request
	var firstRequest []byte
	// First request contains Repository only
	if err := stream.RecvMsg(&firstRequest); err != nil {
		return err
	}
	if err := proto.Unmarshal(firstRequest, &req); err != nil {
		return err
	}

	repoPath, gitConfig, err := s.validateUploadPackRequest(ctx, &req)
	if err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		var stdinBuffer []byte
		err := stream.RecvMsg(&stdinBuffer)
		if err != nil {
			return nil, err
		}
		return stdinBuffer, err
	})

	var totalBytes int64
	var recentBytes int
	const maxBufferSize = 1024 * 1024
	bufferSize := 64 * 1024
	copyBuffer := make([]byte, bufferSize)
	proxy := func(cmd *command.Command) (int64, error) {
		for {
			read, err := cmd.Read(copyBuffer[recentBytes:])
			if err != nil {
				if err == io.EOF {
					if recentBytes > 0 {
						return totalBytes, stream.SendMsg(copyBuffer[:recentBytes])
					}
					return totalBytes, nil
				}
				return totalBytes, err
			}
			totalBytes += int64(read)
			recentBytes += read
			if recentBytes == bufferSize {
				if err := stream.SendMsg(copyBuffer); err != nil {
					return totalBytes, err
				}
				bufferSize *= 2
				if bufferSize > maxBufferSize {
					bufferSize = maxBufferSize
				}
				copyBuffer = make([]byte, bufferSize)
				recentBytes = 0
			}
		}
	}

	if err := s.runUploadPack(ctx, &req, repoPath, gitConfig, stdin, proxy); err != nil {
		return structerr.NewInternal("running upload-pack: %w", err)
	}

	return nil
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

func (s *server) validateUploadPackRequest(ctx context.Context, req postUploadPackRequest) (string, []git.ConfigPair, error) {
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

func (s *server) runUploadPack(ctx context.Context, req postUploadPackRequest, repoPath string, gitConfig []git.ConfigPair, stdin io.Reader, proxy func(*command.Command) (int64, error)) error {
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

	respBytes, err := proxy(cmd)
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
