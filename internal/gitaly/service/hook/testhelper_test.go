package hook

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func newHooksClient(t *testing.T, serverSocketPath string) (gitalypb.HookServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewHookServiceClient(conn), conn
}

type serverOption func(*server)

func runHooksServer(t *testing.T, cfg config.Cfg, serverOpts ...serverOption) (string, func()) {
	return runHooksServerWithAPI(t, gitalyhook.GitlabAPIStub, cfg, serverOpts...)
}

func runHooksServerWithLogger(t *testing.T, cfg config.Cfg, logger *logrus.Logger) (string, func()) {
	srv := testhelper.NewServerWithLogger(t, logger, nil, nil)
	return runHooksServerWithAPIAndTestServer(t, srv, gitalyhook.GitlabAPIStub, cfg)
}

func runHooksServerWithAPI(t *testing.T, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg, serverOpts ...serverOption) (string, func()) {
	return runHooksServerWithAPIAndTestServer(t, testhelper.NewServer(t, nil, nil), gitlabAPI, cfg, serverOpts...)
}

func runHooksServerWithAPIAndTestServer(t *testing.T, srv *testhelper.TestServer, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg, serverOpts ...serverOption) (string, func()) {
	hookServer := NewServer(
		cfg,
		gitalyhook.NewManager(config.NewLocator(cfg), transaction.NewManager(cfg), gitlabAPI, cfg),
		git.NewExecCommandFactory(cfg),
	)
	for _, opt := range serverOpts {
		opt(hookServer.(*server))
	}

	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hookServer)
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}
