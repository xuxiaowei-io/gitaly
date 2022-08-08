//go:build !gitaly_test_sha256

package objectpool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

// clientWithConn allows for passing through the ClientConn to tests which need
// to access other services than ObjectPoolService.
type clientWithConn struct {
	gitalypb.ObjectPoolServiceClient
	conn *grpc.ClientConn
}

// extractConn returns the underlying ClientConn from the client.
func extractConn(client gitalypb.ObjectPoolServiceClient) *grpc.ClientConn {
	return client.(clientWithConn).conn
}

func setup(t *testing.T, ctx context.Context, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, storage.Locator, gitalypb.ObjectPoolServiceClient) {
	t.Helper()

	cfg := testcfg.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)

	locator := config.NewLocator(cfg)
	cfg.SocketPath = runObjectPoolServer(t, cfg, locator, testhelper.NewDiscardingLogger(t), opts...)

	conn, err := grpc.Dial(cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	return cfg, repo, repoPath, locator, clientWithConn{ObjectPoolServiceClient: gitalypb.NewObjectPoolServiceClient(conn), conn: conn}
}

func runObjectPoolServer(t *testing.T, cfg config.Cfg, locator storage.Locator, logger *logrus.Logger, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterObjectPoolServiceServer(srv, NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
			deps.GetHousekeepingManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(), deps.GetPackObjectsConcurrencyTracker(), deps.GetPackObjectsLimiter()))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
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
	}, append(opts, testserver.WithLocator(locator), testserver.WithLogger(logger))...)
}

func newObjectPool(tb testing.TB, cfg config.Cfg, storage, relativePath string) *objectpool.ObjectPool {
	catfileCache := catfile.NewCache(cfg)
	tb.Cleanup(catfileCache.Stop)

	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	pool, err := objectpool.NewObjectPool(
		config.NewLocator(cfg),
		gittest.NewCommandFactory(tb, cfg),
		catfileCache,
		txManager,
		housekeeping.NewManager(cfg.Prometheus, txManager),
		storage,
		relativePath,
	)
	require.NoError(tb, err)

	return pool
}

// initObjectPool creates a new empty object pool in the given storage.
func initObjectPool(tb testing.TB, cfg config.Cfg, storage config.Storage) *objectpool.ObjectPool {
	tb.Helper()

	relativePath := gittest.NewObjectPoolName(tb)
	gittest.InitRepoDir(tb, storage.Path, relativePath)
	catfileCache := catfile.NewCache(cfg)
	tb.Cleanup(catfileCache.Stop)

	pool := newObjectPool(tb, cfg, storage.Name, relativePath)

	poolPath := filepath.Join(storage.Path, relativePath)
	tb.Cleanup(func() { require.NoError(tb, os.RemoveAll(poolPath)) })

	return pool
}

// rewrittenObjectPool returns a pool that is rewritten as if it was passed through Praefect. This should be used
// to access the pool on the disk if the tests are running with Praefect in front of them.
func rewrittenObjectPool(tb testing.TB, ctx context.Context, cfg config.Cfg, pool *objectpool.ObjectPool) *objectpool.ObjectPool {
	replicaPath := gittest.GetReplicaPath(tb, ctx, cfg, pool)
	return newObjectPool(tb, cfg, pool.GetStorageName(), replicaPath)
}
