//go:build !gitaly_test_sha256

package objectpool

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, _, client := setup(t, ctx)
	commitID := gittest.WriteCommit(t, cfg, repoPath)

	txManager := transaction.NewManager(cfg, nil)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	poolProto := &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: gittest.NewObjectPoolName(t),
		},
	}

	_, err := client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: poolProto,
		Origin:     repo,
	})
	require.NoError(t, err)

	pool, err := objectpool.FromProto(
		config.NewLocator(cfg),
		gittest.NewCommandFactory(t, cfg),
		catfileCache,
		txManager,
		housekeeping.NewManager(cfg.Prometheus, txManager),
		&gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: gittest.GetReplicaPath(t, ctx, cfg, poolProto.GetRepository()),
			},
		},
	)
	require.NoError(t, err)
	poolPath := gittest.RepositoryPath(t, pool)

	// Assert that the now-created object pool exists and is valid.
	require.True(t, pool.IsValid())
	require.NoDirExists(t, filepath.Join(poolPath, "hooks"))
	gittest.RequireObjectExists(t, cfg, poolPath, commitID)

	// Making the same request twice should result in an error.
	_, err = client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: poolProto,
		Origin:     repo,
	})
	require.Error(t, err)
	require.True(t, pool.IsValid())
}

func TestCreate_unsuccessful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, _, client := setup(t, ctx, testserver.WithDisablePraefect())

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.CreateObjectPoolRequest
		expectedErr error
	}{
		{
			desc: "no origin repository",
			request: &gitalypb.CreateObjectPoolRequest{
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: gittest.NewObjectPoolName(t),
					},
				},
			},
			expectedErr: errMissingOriginRepository,
		},
		{
			desc: "no object pool",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
			},
			expectedErr: helper.ErrInvalidArgumentf("GetStorageByName: no such storage: %q", ""),
		},
		{
			desc: "outside pools directory",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "outside-pools",
					},
				},
			},
			expectedErr: errInvalidPoolDir,
		},
		{
			desc: "path must be lowercase",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: strings.ToUpper(gittest.NewObjectPoolName(t)),
					},
				},
			},
			expectedErr: errInvalidPoolDir,
		},
		{
			desc: "subdirectories must match first four pool digits",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: "@pools/aa/bb/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.git",
					},
				},
			},
			expectedErr: errInvalidPoolDir,
		},
		{
			desc: "pool path traversal fails",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  cfg.Storages[0].Name,
						RelativePath: gittest.NewObjectPoolName(t) + "/..",
					},
				},
			},
			expectedErr: errInvalidPoolDir,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CreateObjectPool(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
