package blob

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setup(tb testing.TB, ctx context.Context) (config.Cfg, gitalypb.BlobServiceClient) {
	cfg := testcfg.Build(tb)

	addr := testserver.RunGitalyServer(tb, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterBlobServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	})
	cfg.SocketPath = addr

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	tb.Cleanup(func() { conn.Close() })

	return cfg, gitalypb.NewBlobServiceClient(conn)
}
