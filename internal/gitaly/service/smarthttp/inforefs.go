package smarthttp

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

const (
	uploadPackSvc  = "upload-pack"
	receivePackSvc = "receive-pack"
)

func (s *server) InfoRefsUploadPack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})

	return s.infoRefCache.tryCache(stream.Context(), in, w, func(w io.Writer) error {
		return s.handleInfoRefs(stream.Context(), uploadPackSvc, repoPath, in, w)
	})
}

func (s *server) InfoRefsReceivePack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	repository := in.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})
	return s.handleInfoRefs(stream.Context(), receivePackSvc, repoPath, in, w)
}

func (s *server) InfoRefsPollRef(ctx context.Context, req *gitalypb.InfoRefsPollRefRequest) (*gitalypb.InfoRefsPollRefResponse, error) {
	repository := req.GetRepository()
	if err := s.locator.ValidateRepository(repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	defer wg.Wait()
	// Close the read end of the pipe to abort any writes. This is needed to unblock the goroutine if
	// we exit early before it finishes writing. This must be done before wg.Wait().
	defer pr.CloseWithError(nil) // always returns nil
	wg.Add(1)
	go func() {
		defer wg.Done()
		in := &gitalypb.InfoRefsRequest{
			Repository:       req.Repository,
			GitConfigOptions: req.GitConfigOptions,
			GitProtocol:      req.GitProtocol,
		}
		err := s.infoRefCache.tryCache(ctx, in, pw, func(w io.Writer) error {
			return s.handleInfoRefs(ctx, uploadPackSvc, repoPath, in, w)
		})
		_ = pw.CloseWithError(err)
	}()
	rd, err := stats.ParseReferenceDiscovery(pr)
	if err != nil {
		if _, ok := err.(structerr.Error); ok {
			return nil, err
		}
		return nil, structerr.NewInternal("parse ref discovery: %w", err)
	}
	if len(rd.Refs) == 0 {
		return &gitalypb.InfoRefsPollRefResponse{
			EmptyRepo: true,
		}, nil
	}
	var wanted, defaultRef, legacyDefaultRef *stats.Reference
	wantRefName := string(req.Ref)
loop:
	for i := range rd.Refs {
		r := &rd.Refs[i]
		switch r.Name {
		case wantRefName:
			wanted = r
			break loop
		case string(git.DefaultRef):
			defaultRef = r
		case string(git.LegacyDefaultRef):
			legacyDefaultRef = r
		}
	}
	if wanted == nil { // not found
		switch {
		case wantRefName != "HEAD":
			// was looking for a certain ref, but didn't find it.
			return nil, structerr.NewNotFound("ref %s not found", wantRefName)
		case defaultRef != nil:
			wanted = defaultRef
		case legacyDefaultRef != nil:
			wanted = legacyDefaultRef
		default:
			return nil, structerr.NewNotFound("default branch not found")
		}
	}
	return &gitalypb.InfoRefsPollRefResponse{
		CommitId: wanted.Oid,
	}, nil
}

func (s *server) handleInfoRefs(ctx context.Context, service, repoPath string, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"service": service,
	}).Debug("handleInfoRefs")

	cmdOpts := []git.CmdOpt{git.WithGitProtocol(req)}
	if service == "receive-pack" {
		cmdOpts = append(cmdOpts, git.WithRefTxHook(req.Repository))
	}

	config, err := git.ConvertConfigOptions(req.GitConfigOptions)
	if err != nil {
		return err
	}
	cmdOpts = append(cmdOpts, git.WithConfig(config...))

	cmd, err := s.gitCmdFactory.New(ctx, req.GetRepository(), git.Command{
		Name:  service,
		Flags: []git.Option{git.Flag{Name: "--stateless-rpc"}, git.Flag{Name: "--advertise-refs"}},
		Args:  []string{repoPath},
	}, cmdOpts...)
	if err != nil {
		return structerr.NewInternal("cmd: %w", err)
	}

	if _, err := pktline.WriteString(w, fmt.Sprintf("# service=git-%s\n", service)); err != nil {
		return structerr.NewInternal("pktLine: %w", err)
	}

	if err := pktline.WriteFlush(w); err != nil {
		return structerr.NewInternal("pktFlush: %w", err)
	}

	if _, err := io.Copy(w, cmd); err != nil {
		return structerr.NewInternal("send: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return structerr.NewInternal("wait: %w", err)
	}

	return nil
}
