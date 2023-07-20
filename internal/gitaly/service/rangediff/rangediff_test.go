package rangediff

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupRangeDiffService(tb testing.TB, ctx context.Context, opt ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, gitalypb.RangeDiffServiceClient) {
	cfg := testcfg.Build(tb)

	addr := testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRangeDiffServiceServer(srv, NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
		))
	}, opt...)
	cfg.SocketPath = addr

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	tb.Cleanup(func() { testhelper.MustClose(tb, conn) })

	client := gitalypb.NewRangeDiffServiceClient(conn)

	repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	return cfg, repo, repoPath, client
}

func TestRawRangeDiff_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, _, client := setupRangeDiffService(t, ctx)
	repoProto, _ = gittest.CreateRepository(t, ctx, cfg)

	request := &gitalypb.RangeDiffRequest{
		Repository:    repoProto,
		Rev1OrRange1:  "master~2",
		Rev2OrRange2:  "master",
		RangeNotation: gitalypb.RangeDiffRequest_TWO_REVS,
	}

	stream, err := client.RawRangeDiff(ctx, request)
	require.NoError(t, err)

	var buffer bytes.Buffer
	writer := io.Writer(&buffer)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		_, err = writer.Write(resp.GetData())
		require.NoError(t, err)
	}
}

func TestRawRangeDiff_inputValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, _, client := setupRangeDiffService(t, ctx)
	repoProto, _ = gittest.CreateRepository(t, ctx, cfg)

	tests := []struct {
		desc        string
		request     *gitalypb.RangeDiffRequest
		expectedErr error
	}{
		{
			desc: "empty rev1 or range1",
			request: &gitalypb.RangeDiffRequest{
				Repository:    repoProto,
				Rev2OrRange2:  "master",
				RangeNotation: gitalypb.RangeDiffRequest_TWO_REVS,
			},
			expectedErr: structerr.NewInvalidArgument("revisions cannot be empty"),
		},
		{
			desc: "empty rev2 or range2",
			request: &gitalypb.RangeDiffRequest{
				Repository:    repoProto,
				Rev1OrRange1:  "master~2",
				RangeNotation: gitalypb.RangeDiffRequest_TWO_REVS,
			},
			expectedErr: structerr.NewInvalidArgument("revisions cannot be empty"),
		},
		{
			desc: "unsupported range notation",
			request: &gitalypb.RangeDiffRequest{
				Repository:   repoProto,
				Rev1OrRange1: "master~2",
				Rev2OrRange2: "master",
			},
			expectedErr: structerr.NewInvalidArgument("only TWO_REVS range notation is supported"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.RawRangeDiff(ctx, tc.request)
			require.NoError(t, err)
			err = drainRawRangeDiffResponse(stream)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func drainRawRangeDiffResponse(c gitalypb.RangeDiffService_RawRangeDiffClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
