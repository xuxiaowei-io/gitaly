//go:build !gitaly_test_sha256

package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLink(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, _, _, client := setup(t, ctx, testserver.WithDisablePraefect())

	localRepo := localrepo.NewTestRepo(t, cfg, repo)
	poolProto, pool := createObjectPool(t, ctx, cfg, client, repo)

	// Mock object in the pool, which should be available to the pool members
	// after linking
	poolCommitID := gittest.WriteCommit(t, cfg, pool.FullPath(),
		gittest.WithBranch("pool-test-branch"))

	testCases := []struct {
		desc string
		req  *gitalypb.LinkRepositoryToObjectPoolRequest
		code codes.Code
	}{
		{
			desc: "Repository does not exist",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: nil,
				ObjectPool: poolProto,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Pool does not exist",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: repo,
				ObjectPool: nil,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Successful request",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: repo,
				ObjectPool: poolProto,
			},
			code: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.LinkRepositoryToObjectPool(ctx, tc.req)

			if tc.code != codes.OK {
				testhelper.RequireGrpcCode(t, err, tc.code)
				return
			}

			require.NoError(t, err, "error from LinkRepositoryToObjectPool")

			commit, err := localRepo.ReadCommit(ctx, git.Revision(poolCommitID))
			require.NoError(t, err)
			require.NotNil(t, commit)
			require.Equal(t, poolCommitID.String(), commit.Id)
		})
	}
}

func TestLink_idempotent(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repoProto, _, _, client := setup(t, ctx)

	poolProto, _ := createObjectPool(t, ctx, cfg, client, repoProto)

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repoProto,
		ObjectPool: poolProto,
	}

	_, err := client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)
}

func TestLink_noClobber(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, _, client := setup(t, ctx)
	poolProto, _ := createObjectPool(t, ctx, cfg, client, repoProto)

	alternatesFile := filepath.Join(repoPath, "objects/info/alternates")
	require.NoFileExists(t, alternatesFile)

	contentBefore := "mock/objects\n"
	require.NoError(t, os.WriteFile(alternatesFile, []byte(contentBefore), 0o644))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repoProto,
		ObjectPool: poolProto,
	}

	_, err := client.LinkRepositoryToObjectPool(ctx, request)
	require.Error(t, err)

	contentAfter := testhelper.MustReadFile(t, alternatesFile)
	require.Equal(t, contentBefore, string(contentAfter), "contents of existing alternates file should not have changed")
}

func TestLink_noPool(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg, repo, _, _, client := setup(t, ctx)

	_, err := client.LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repo,
		ObjectPool: &gitalypb.ObjectPool{
			Repository: &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: gittest.NewObjectPoolName(t),
			},
		},
	})
	testhelper.RequireGrpcCode(t, err, codes.NotFound)
	require.Error(t, err, "GetRepoPath: not a git repository:")
}
