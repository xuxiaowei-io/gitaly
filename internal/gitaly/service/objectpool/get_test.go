//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetObjectPoolSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, _, _, client := setup(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	_, pool, _ := createObjectPool(t, ctx, cfg, client, repoProto)
	relativePoolPath := pool.GetRelativePath()
	require.NoError(t, pool.Link(ctx, repo))

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
	require.NoError(t, os.MkdirAll(filepath.Dir(alternatesFilePath), perm.SharedDir))
	require.NoError(t, os.WriteFile(alternatesFilePath, []byte("not-a-directory"), perm.SharedFile))

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}

func TestGetObjectPool_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, _, _, _, client := setup(t, ctx)
	for _, tc := range []struct {
		desc        string
		req         *gitalypb.GetObjectPoolRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.GetObjectPoolRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.GetObjectPool(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
