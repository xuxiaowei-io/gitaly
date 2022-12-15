package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v15/auth"
	gclient "gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	internalclient "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var testTime = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func newRepositoryClient(tb testing.TB, cfg config.Cfg, serverSocketPath string) gitalypb.RepositoryServiceClient {
	connOpts := []grpc.DialOption{
		internalclient.UnaryInterceptor(), internalclient.StreamInterceptor(),
	}
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := gclient.Dial(serverSocketPath, connOpts)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}

func newObjectPoolClient(tb testing.TB, cfg config.Cfg, serverSocketPath string) gitalypb.ObjectPoolServiceClient {
	conn, err := gclient.Dial(serverSocketPath, nil)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewObjectPoolServiceClient(conn)
}

func newMuxedRepositoryClient(t *testing.T, ctx context.Context, cfg config.Cfg, serverSocketPath string, handshaker internalclient.Handshaker) gitalypb.RepositoryServiceClient {
	conn, err := internalclient.Dial(ctx, serverSocketPath, []grpc.DialOption{
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)),
	}, handshaker)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewRepositoryServiceClient(conn)
}

func assertModTimeAfter(t *testing.T, afterTime time.Time, paths ...string) bool {
	t.Helper()
	// NOTE: Since some filesystems don't have sub-second precision on `mtime`
	//       we're rounding the times to seconds
	afterTime = afterTime.Round(time.Second)
	for _, path := range paths {
		s, err := os.Stat(path)
		assert.NoError(t, err)

		if !s.ModTime().Round(time.Second).After(afterTime) {
			t.Errorf("ModTime is not after afterTime: %q < %q", s.ModTime().Round(time.Second).String(), afterTime.String())
		}
	}
	return t.Failed()
}

func runRepositoryService(tb testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, opts ...testserver.GitalyServerOpt) (gitalypb.RepositoryServiceClient, string) {
	serverSocketPath := testserver.RunGitalyServer(tb, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(
			cfg,
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetHousekeepingManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache(), deps.GetPackObjectsConcurrencyTracker(), deps.GetPackObjectsLimiter()))
		gitalypb.RegisterRemoteServiceServer(srv, remote.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
			deps.GetConnsPool(),
		))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterObjectPoolServiceServer(srv, objectpool.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
			deps.GetHousekeepingManager(),
		))
	}, opts...)

	return newRepositoryClient(tb, cfg, serverSocketPath), serverSocketPath
}

func setupRepositoryService(tb testing.TB, ctx context.Context, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	cfg, client := setupRepositoryServiceWithoutRepo(tb, opts...)

	repo, repoPath := gittest.CreateRepository(tb, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})
	return cfg, repo, repoPath, client
}

func setupRepositoryServiceWithoutRepo(tb testing.TB, opts ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.RepositoryServiceClient) {
	cfg := testcfg.Build(tb)

	testcfg.BuildGitalyHooks(tb, cfg)
	testcfg.BuildGitalySSH(tb, cfg)

	client, serverSocketPath := runRepositoryService(tb, cfg, nil, opts...)
	cfg.SocketPath = serverSocketPath

	return cfg, client
}

func requireCommitGraphInfo(tb testing.TB, repoPath string, expectedInfo stats.CommitGraphInfo) {
	tb.Helper()

	commitGraphInfo, err := stats.CommitGraphInfoForRepository(repoPath)
	require.NoError(tb, err)
	require.Equal(tb, expectedInfo, commitGraphInfo)
}

func requireRepositoryInfoLog(tb testing.TB, entries ...*logrus.Entry) {
	tb.Helper()

	const key = "repository_info"
	for _, entry := range entries {
		if entry.Message == "repository info" {
			require.Contains(tb, entry.Data, "grpc.request.glProjectPath")
			require.Contains(tb, entry.Data, "grpc.request.glRepository")
			require.Contains(tb, entry.Data, key, "objects info not found")
			require.IsType(tb, stats.RepositoryInfo{}, entry.Data[key])
			return
		}
	}
	require.FailNow(tb, "no info about statistics")
}
