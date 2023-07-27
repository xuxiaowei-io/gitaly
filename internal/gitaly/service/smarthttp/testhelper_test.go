package smarthttp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func startSmartHTTPServerWithOptions(t *testing.T, cfg config.Cfg, opts []ServerOpt, serverOpts []testserver.GitalyServerOpt) testserver.GitalyServer {
	return testserver.StartGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSmartHTTPServiceServer(srv, NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetDiskCache(),
			opts...,
		))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetHousekeepingManager(),
			deps.GetBackupSink(),
			deps.GetBackupLocator(),
			deps.GetRepositoryCounter(),
		))
		gitalypb.RegisterObjectPoolServiceServer(srv, objectpool.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
			deps.GetHousekeepingManager(),
			deps.GetRepositoryCounter(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(
			deps.GetHookManager(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
			deps.GetPackObjectsLimiter(),
		))
	}, serverOpts...)
}

func startSmartHTTPServer(t *testing.T, cfg config.Cfg, opts ...ServerOpt) testserver.GitalyServer {
	return startSmartHTTPServerWithOptions(t, cfg, opts, nil)
}

func runSmartHTTPServer(t *testing.T, cfg config.Cfg, opts ...ServerOpt) string {
	gitalyServer := startSmartHTTPServer(t, cfg, opts...)
	return gitalyServer.Address()
}

func newSmartHTTPClient(t *testing.T, serverSocketPath, token string) gitalypb.SmartHTTPServiceClient {
	t.Helper()

	conn, err := grpc.Dial(serverSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		testhelper.MustClose(t, conn)
	})

	return gitalypb.NewSmartHTTPServiceClient(conn)
}

func newMuxedSmartHTTPClient(t *testing.T, ctx context.Context, serverSocketPath, token string, serverFactory backchannel.ServerFactory) gitalypb.SmartHTTPServiceClient {
	t.Helper()

	conn, err := client.Dial(
		ctx,
		serverSocketPath,
		client.WithGrpcOptions([]grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))}),
		client.WithHandshaker(backchannel.NewClientHandshaker(testhelper.NewDiscardingLogEntry(t), serverFactory, backchannel.DefaultConfiguration())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return gitalypb.NewSmartHTTPServiceClient(conn)
}
