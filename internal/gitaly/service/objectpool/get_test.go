//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestGetObjectPoolSuccess(t *testing.T) {
	t.Parallel()

	poolCtx := testhelper.Context(t)
	cfg, repoProto, _, _, client := setup(t, poolCtx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	relativePoolPath := pool.GetRelativePath()

	require.NoError(t, pool.Create(poolCtx, repo))
	require.NoError(t, pool.Link(poolCtx, repo))

	ctx := testhelper.Context(t)

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repoProto,
	})

	require.NoError(t, err)
	require.Equal(t, relativePoolPath, resp.GetObjectPool().GetRepository().GetRelativePath())
}

func TestGetObjectPoolNoFile(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, _, client := setup(t, ctx)

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}

func TestGetObjectPoolBadFile(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, _, client := setup(t, ctx)

	alternatesFilePath := filepath.Join(repoPath, "objects", "info", "alternates")
	require.NoError(t, os.MkdirAll(filepath.Dir(alternatesFilePath), 0o755))
	require.NoError(t, os.WriteFile(alternatesFilePath, []byte("not-a-directory"), 0o644))

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}
