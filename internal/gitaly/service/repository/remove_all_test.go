package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestRemoveAll(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryServiceWithoutRepo(t)
	ctx := testhelper.Context(t)

	_, repo1Path := gittest.CreateRepository(t, ctx, cfg)
	_, repo2Path := gittest.CreateRepository(t, ctx, cfg)

	require.DirExists(t, repo1Path)
	require.DirExists(t, repo1Path)

	_, err := client.RemoveAll(ctx, &gitalypb.RemoveAllRequest{
		StorageName: cfg.Storages[0].Name,
	})
	require.NoError(t, err)

	require.NoDirExists(t, repo1Path)
	require.NoDirExists(t, repo2Path)
}
