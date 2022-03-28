package internalgitaly

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestCleanRepos(t *testing.T) {
	testhelper.SkipWithPraefect(t, "internal RPC that will never go through Praefect")

	cfg := testcfg.Build(t)
	storageName := cfg.Storages[0].Name
	storageRoot := cfg.Storages[0].Path
	internalService := NewServer([]config.Storage{{Name: storageName, Path: storageRoot}})

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterInternalGitalyServer(srv, internalService)
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
	})

	ctx := testhelper.Context(t)

	conn, err := grpc.Dial(cfg.SocketPath, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	client := gitalypb.NewInternalGitalyClient(conn)

	logger, _ := test.NewNullLogger()
	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))
	t.Run("success", func(t *testing.T) {
		deletedRepo, deletedRepoPath := gittest.CreateRepository(ctx, t, cfg)
		deletedRelPath := deletedRepo.RelativePath
		_, nonDeletedRepoPath := gittest.CreateRepository(ctx, t, cfg)

		_, err = client.CleanRepos(ctx, &gitalypb.CleanReposRequest{
			StorageName:   storageName,
			RelativePaths: []string{deletedRelPath},
		})
		require.NoError(t, err)

		repoMovedToDir := filepath.Join(
			storageRoot,
			"+gitaly",
			"lost+found",
			time.Now().Format("2006-01-02"),
			deletedRelPath,
		)

		gitDirs, err := filepath.Glob(repoMovedToDir + "*")
		require.NoError(t, err)
		require.Len(t, gitDirs, 1)
		assert.True(t, storage.IsGitDirectory(gitDirs[0]))
		assert.NoDirExists(t, deletedRepoPath)
		assert.DirExists(t, nonDeletedRepoPath)
	})

	t.Run("missing source", func(t *testing.T) {
		_, err = client.CleanRepos(ctx, &gitalypb.CleanReposRequest{
			StorageName:   storageName,
			RelativePaths: []string{"/path/doesnt/exist"},
		})
		require.NoError(t, err)
	})
}
