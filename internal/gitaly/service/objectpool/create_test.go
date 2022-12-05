//go:build !gitaly_test_sha256

package objectpool

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
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

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	poolReq := &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
	}

	_, err := client.CreateObjectPool(ctx, poolReq)
	require.NoError(t, err)

	pool = rewrittenObjectPool(t, ctx, cfg, pool)

	// Assert that the now-created object pool exists and is valid.
	require.True(t, pool.IsValid())
	require.NoDirExists(t, filepath.Join(pool.FullPath(), "hooks"))
	gittest.RequireObjectExists(t, cfg, pool.FullPath(), commitID)

	// Making the same request twice should result in an error.
	_, err = client.CreateObjectPool(ctx, poolReq)
	require.Error(t, err)
	require.True(t, pool.IsValid())
}

func TestCreate_unsuccessful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, _, client := setup(t, ctx, testserver.WithDisablePraefect())

	storageName := repo.GetStorageName()
	pool := initObjectPool(t, cfg, cfg.Storages[0])
	validPoolPath := pool.GetRelativePath()

	testCases := []struct {
		desc        string
		request     *gitalypb.CreateObjectPoolRequest
		expectedErr error
	}{
		{
			desc: "no origin repository",
			request: &gitalypb.CreateObjectPoolRequest{
				ObjectPool: pool.ToProto(),
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
						StorageName:  storageName,
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
						StorageName:  storageName,
						RelativePath: strings.ToUpper(validPoolPath),
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
						StorageName:  storageName,
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
						StorageName:  storageName,
						RelativePath: validPoolPath + "/..",
					},
				},
			},
			expectedErr: errInvalidPoolDir,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CreateObjectPool(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
