//go:build !gitaly_test_sha256

package housekeeping

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func requireObjectCount(t *testing.T, ctx context.Context, repo *localrepo.Repo, expectedObjects uint64) {
	t.Helper()

	objects, err := stats.LooseObjects(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, expectedObjects, objects)
}

func requirePackfileCount(t *testing.T, repoPath string, expectedPackfiles int) {
	t.Helper()

	packfiles, err := stats.PackfilesCount(repoPath)
	require.NoError(t, err)
	require.Equal(t, expectedPackfiles, packfiles)
}

func TestRepackObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("no server info is written", func(t *testing.T) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

		requireObjectCount(t, ctx, repo, 2)
		requirePackfileCount(t, repoPath, 0)

		require.NoError(t, RepackObjects(ctx, repo, RepackObjectsConfig{}))

		requireObjectCount(t, ctx, repo, 0)
		requirePackfileCount(t, repoPath, 1)

		require.NoFileExists(t, filepath.Join(repoPath, "info", "refs"))
		require.NoFileExists(t, filepath.Join(repoPath, "objects", "info", "packs"))
	})

	testRepoAndPool(t, "delta islands", func(t *testing.T, relativePath string) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           relativePath,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.TestDeltaIslands(t, cfg, repoPath, repoPath, IsPoolRepository(repoProto), func() error {
			return RepackObjects(ctx, repo, RepackObjectsConfig{
				FullRepack: true,
			})
		})
	})
}
