package config_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestConfigLocator_GetRepoPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	const storageName = "exists"
	cfg := testcfg.Build(t, testcfg.WithStorages(storageName, "removed"))
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, setup.RegisterAll)
	locator := config.NewLocator(cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	if testhelper.IsPraefectEnabled() {
		repo.RelativePath = strings.TrimPrefix(repoPath, cfg.Storages[0].Path)
	}

	// The storage name still present in the storages list, but not on the disk.
	require.NoError(t, os.RemoveAll(cfg.Storages[1].Path))

	// The repository path exists on the disk, but it is not a git repository.
	const notRepositoryFolder = "not-a-git-repo"
	require.NoError(t, os.MkdirAll(filepath.Join(cfg.Storages[0].Path, notRepositoryFolder), perm.SharedDir))

	for _, tc := range []struct {
		desc    string
		repo    *gitalypb.Repository
		opts    []storage.GetRepoPathOption
		expPath string
		expErr  error
	}{
		{
			desc:   "storage is empty",
			repo:   &gitalypb.Repository{RelativePath: repo.RelativePath},
			expErr: structerr.NewInvalidArgument(`GetStorageByName: no such storage: ""`),
		},
		{
			desc:   "unknown storage",
			repo:   &gitalypb.Repository{StorageName: "invalid", RelativePath: repo.RelativePath},
			expErr: structerr.NewInvalidArgument(`GetStorageByName: no such storage: "invalid"`),
		},
		{
			desc: "storage doesn't exist on disk",
			repo: &gitalypb.Repository{StorageName: cfg.Storages[1].Name, RelativePath: repo.RelativePath},
			expErr: structerr.NewNotFound(`GetPath: does not exist: %w`, &fs.PathError{
				Op:   "stat",
				Path: cfg.Storages[1].Path,
				Err:  syscall.ENOENT,
			}),
		},
		{
			desc:   "relative path is empty",
			repo:   &gitalypb.Repository{StorageName: storageName, RelativePath: ""},
			expErr: structerr.NewInvalidArgument("GetPath: relative path missing"),
		},
		{
			desc:   "unknown relative path",
			repo:   &gitalypb.Repository{StorageName: storageName, RelativePath: "invalid"},
			expErr: structerr.NewNotFound(`GetRepoPath: not a git repository: %q`, filepath.Join(cfg.Storages[0].Path, "invalid")),
		},
		{
			desc:   "path exists but not a git repository",
			repo:   &gitalypb.Repository{StorageName: storageName, RelativePath: notRepositoryFolder},
			expErr: structerr.NewNotFound(`GetRepoPath: not a git repository: %q`, filepath.Join(cfg.Storages[0].Path, notRepositoryFolder)),
		},
		{
			desc:    "unknown relative path without repo verification",
			repo:    &gitalypb.Repository{StorageName: storageName, RelativePath: "invalid"},
			opts:    []storage.GetRepoPathOption{storage.WithRepositoryVerificationSkipped()},
			expPath: filepath.Join(cfg.Storages[0].Path, "invalid"),
		},
		{
			desc:    "path exists but not a git repository without repo verification",
			repo:    &gitalypb.Repository{StorageName: storageName, RelativePath: notRepositoryFolder},
			opts:    []storage.GetRepoPathOption{storage.WithRepositoryVerificationSkipped()},
			expPath: filepath.Join(cfg.Storages[0].Path, notRepositoryFolder),
		},
		{
			desc:   "relative path escapes parent folder",
			repo:   &gitalypb.Repository{StorageName: storageName, RelativePath: "../.."},
			expErr: structerr.NewInvalidArgument(`GetRepoPath: %w`, fmt.Errorf("relative path escapes root directory")),
		},
		{
			desc:    "proper repository path",
			repo:    repo,
			expPath: filepath.Join(cfg.Storages[0].Path, repo.GetRelativePath()),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path, err := locator.GetRepoPath(tc.repo, tc.opts...)
			require.Equal(t, tc.expPath, path)
			require.Equal(t, tc.expErr, err)
		})
	}
}

func TestConfigLocator_CacheDir(t *testing.T) {
	t.Parallel()
	const storageName = "exists"
	cfg := testcfg.Build(t, testcfg.WithStorages(storageName, "removed"))
	locator := config.NewLocator(cfg)

	t.Run("storage exists", func(t *testing.T) {
		path, err := locator.CacheDir(storageName)
		require.NoError(t, err)
		require.Equal(t, path, filepath.Join(cfg.Storages[0].Path, "+gitaly/cache"))
	})

	t.Run("storage doesn't exist on disk", func(t *testing.T) {
		require.NoError(t, os.RemoveAll(cfg.Storages[1].Path))
		path, err := locator.CacheDir(cfg.Storages[1].Name)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(cfg.Storages[1].Path, "+gitaly/cache"), path)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := locator.CacheDir("unknown")
		require.Equal(t, structerr.NewInvalidArgument(`cache dir: no such storage: "unknown"`), err)
	})
}

func TestConfigLocator_StateDir(t *testing.T) {
	t.Parallel()
	const storageName = "exists"
	cfg := testcfg.Build(t, testcfg.WithStorages(storageName, "removed"))
	locator := config.NewLocator(cfg)

	t.Run("storage exists", func(t *testing.T) {
		path, err := locator.StateDir(storageName)
		require.NoError(t, err)
		require.Equal(t, path, filepath.Join(cfg.Storages[0].Path, "+gitaly/state"))
	})

	t.Run("storage doesn't exist on disk", func(t *testing.T) {
		require.NoError(t, os.RemoveAll(cfg.Storages[1].Path))
		path, err := locator.StateDir(cfg.Storages[1].Name)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(cfg.Storages[1].Path, "+gitaly/state"), path)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := locator.StateDir("unknown")
		require.Equal(t, structerr.NewInvalidArgument(`state dir: no such storage: "unknown"`), err)
	})
}

func TestConfigLocator_TempDir(t *testing.T) {
	t.Parallel()
	const storageName = "exists"
	cfg := testcfg.Build(t, testcfg.WithStorages(storageName, "removed"))
	locator := config.NewLocator(cfg)

	t.Run("storage exists", func(t *testing.T) {
		path, err := locator.TempDir(storageName)
		require.NoError(t, err)
		require.Equal(t, path, filepath.Join(cfg.Storages[0].Path, "+gitaly/tmp"))
	})

	t.Run("storage doesn't exist on disk", func(t *testing.T) {
		require.NoError(t, os.RemoveAll(cfg.Storages[1].Path))
		path, err := locator.TempDir(cfg.Storages[1].Name)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(cfg.Storages[1].Path, "+gitaly/tmp"), path)
	})

	t.Run("unknown storage", func(t *testing.T) {
		_, err := locator.TempDir("unknown")
		require.Equal(t, structerr.NewInvalidArgument(`tmp dir: no such storage: "unknown"`), err)
	})
}
