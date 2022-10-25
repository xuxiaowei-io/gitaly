//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestNewObjectPool(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	locator := config.NewLocator(cfg)

	t.Run("successful", func(t *testing.T) {
		_, err := NewObjectPool(locator, nil, nil, nil, nil, cfg.Storages[0].Name, gittest.NewObjectPoolName(t))
		require.NoError(t, err)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := NewObjectPool(locator, nil, nil, nil, nil, "mepmep", gittest.NewObjectPoolName(t))
		require.Equal(t, helper.ErrInvalidArgumentf("GetStorageByName: no such storage: %q", "mepmep"), err)
	})
}

func TestFromRepo_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	locator := config.NewLocator(cfg)

	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))

	poolFromRepo, err := FromRepo(locator, pool.gitCmdFactory, nil, nil, nil, repo)
	require.NoError(t, err)
	require.Equal(t, pool.relativePath, poolFromRepo.relativePath)
	require.Equal(t, pool.storageName, poolFromRepo.storageName)
}

func TestFromRepo_failures(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	t.Run("without alternates file", func(t *testing.T) {
		cfg, pool, repoProto := setupObjectPool(t, ctx)
		locator := config.NewLocator(cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		poolFromRepo, err := FromRepo(locator, pool.gitCmdFactory, nil, nil, nil, repo)
		require.Equal(t, ErrAlternateObjectDirNotExist, err)
		require.Nil(t, poolFromRepo)
	})

	for _, tc := range []struct {
		desc        string
		fileContent []byte
		expectedErr error
	}{
		{
			desc:        "alternates points to non existent path",
			fileContent: []byte("/tmp/invalid_path"),
			expectedErr: ErrInvalidPoolRepository,
		},
		{
			desc:        "alternates is empty",
			fileContent: nil,
			expectedErr: nil,
		},
		{
			desc:        "alternates is commented",
			fileContent: []byte("#/tmp/invalid/path"),
			expectedErr: ErrAlternateObjectDirNotExist,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, pool, repoProto := setupObjectPool(t, ctx)
			locator := config.NewLocator(cfg)

			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			repoPath, err := repo.Path()
			require.NoError(t, err)

			require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "info"), 0o755))
			alternateFilePath := filepath.Join(repoPath, "objects", "info", "alternates")
			require.NoError(t, os.WriteFile(alternateFilePath, tc.fileContent, 0o644))
			poolFromRepo, err := FromRepo(locator, pool.gitCmdFactory, nil, nil, nil, repo)
			require.Equal(t, tc.expectedErr, err)
			require.Nil(t, poolFromRepo)

			require.NoError(t, os.Remove(alternateFilePath))
		})
	}
}

func TestCreate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	repoPath, err := repo.Path()
	require.NoError(t, err)

	commitID := gittest.ResolveRevision(t, cfg, repoPath, "refs/heads/master")

	require.NoError(t, pool.Create(ctx, repo))
	require.True(t, pool.IsValid())

	// There should not be a "hooks" directory in the pool.
	require.NoDirExists(t, filepath.Join(pool.FullPath(), "hooks"))
	// The "origin" remote of the pool points to the pool member.
	require.Equal(t, repoPath, text.ChompBytes(gittest.Exec(t, cfg, "-C", pool.FullPath(), "remote", "get-url", "origin")))
	// The "master" branch points to the same commit as in the pool member.
	require.Equal(t, commitID, gittest.ResolveRevision(t, cfg, pool.FullPath(), "refs/heads/master"))
	// Objects exist in the pool repository.
	gittest.RequireObjectExists(t, cfg, pool.FullPath(), commitID)
}

func TestCreate_subdirsExist(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	err := pool.Create(ctx, repo)
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx))

	// Recreate pool so the subdirs exist already
	err = pool.Create(ctx, repo)
	require.NoError(t, err)
}

func TestRemove(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	err := pool.Create(ctx, repo)
	require.NoError(t, err)

	require.True(t, pool.Exists())
	require.NoError(t, pool.Remove(ctx))
	require.False(t, pool.Exists())
}
