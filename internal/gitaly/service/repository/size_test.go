//go:build !gitaly_test_sha256

package repository

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// We assume that the combined size of the Git objects in the test
// repository, even in optimally packed state, is greater than this.
const testRepoMinSizeKB = 10000

func TestSuccessfulRepositorySizeRequestPoolMember(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	repoClient, serverSocketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = serverSocketPath

	objectPoolClient := newObjectPoolClient(t, cfg, serverSocketPath)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	sizeRequest := &gitalypb.RepositorySizeRequest{Repository: repo}
	response, err := repoClient.RepositorySize(ctx, sizeRequest)
	require.NoError(t, err)

	sizeBeforePool := response.GetSize()

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	poolProto := &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: gittest.NewObjectPoolName(t),
		},
	}

	_, err = objectPoolClient.CreateObjectPool(
		ctx,
		&gitalypb.CreateObjectPoolRequest{
			ObjectPool: poolProto,
			Origin:     repo,
		})
	require.NoError(t, err)

	_, err = objectPoolClient.LinkRepositoryToObjectPool(
		ctx,
		&gitalypb.LinkRepositoryToObjectPoolRequest{
			Repository: repo,
			ObjectPool: poolProto,
		},
	)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "gc")

	response, err = repoClient.RepositorySize(ctx, sizeRequest)
	require.NoError(t, err)
	assert.Less(t, response.GetSize(), sizeBeforePool)
}

func TestSuccessfulRepositorySizeRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	request := &gitalypb.RepositorySizeRequest{Repository: repo}
	response, err := client.RepositorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)

	var blob [16 * 1024]byte
	rand.Read(blob[:])

	treeOID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{
			Mode:    "100644",
			Path:    "1kbblob",
			Content: string(blob[:]),
		},
	})
	commitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTree(treeOID))

	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/keep-around/keep1"), commitOID)
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/merge-requests/1123"), commitOID)
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/pipelines/pipeline2"), commitOID)
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/environments/env1"), commitOID)

	responseAfterRefs, err := client.RepositorySize(ctx, request)
	require.NoError(t, err)
	assert.Less(t, response.Size, responseAfterRefs.Size)
}

func TestFailedRepositorySizeRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		description string
		repo        *gitalypb.Repository
		expectedErr error
	}{
		{
			description: "no repository provided",
			repo:        nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.RepositorySizeRequest{Repository: testCase.repo}
			_, err := client.RepositorySize(ctx, request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func BenchmarkRepositorySize(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg, client := setupRepositoryServiceWithoutRepo(b)

	for _, tc := range []struct {
		desc  string
		setup func(b *testing.B) *gitalypb.Repository
	}{
		{
			desc: "empty repository",
			setup: func(b *testing.B) *gitalypb.Repository {
				repo, _ := gittest.CreateRepository(b, ctx, cfg)
				return repo
			},
		},
		{
			desc: "benchmark repository",
			setup: func(b *testing.B) *gitalypb.Repository {
				repo, _ := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
					Seed: "benchmark.git",
				})
				return repo
			},
		},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			repo := tc.setup(b)

			b.StartTimer()

			for i := 0; i < b.N; i++ {
				_, err := client.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{
					Repository: repo,
				})
				require.NoError(b, err)
			}
		})
	}
}

func TestRepositorySize_SuccessfulGetObjectDirectorySizeRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(t, ctx)
	repo.GitObjectDirectory = "objects/"

	request := &gitalypb.GetObjectDirectorySizeRequest{Repository: repo}
	response, err := client.GetObjectDirectorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)
}

func TestRepositorySize_GetObjectDirectorySize_quarantine(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	locator := config.NewLocator(cfg)

	t.Run("quarantined repo", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})

		quarantine, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo), locator)
		require.NoError(t, err)

		// quarantine.New in Gitaly would receive an already rewritten repository. Gitaly would then calculate
		// the quarantine directories based on the rewritten relative path. That quarantine would then be looped
		// through Rails, which would then send a request with the quarantine object directories set based on the
		// rewritten relative path but with the original relative path of the repository. Since we're using the production
		// helpers here, we need to manually substitute the rewritten relative path with the original one when sending
		// it back through the API.
		quarantinedRepo := quarantine.QuarantinedRepo()
		quarantinedRepo.RelativePath = repo.RelativePath

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: quarantinedRepo,
		})
		require.NoError(t, err)
		require.NotNil(t, response)

		// Due to platform incompatibilities we can't assert the exact size of bytes: on
		// some, the directory entry is counted, on some it's not.
		require.Less(t, response.Size, int64(10))
	})

	t.Run("quarantined repo with different relative path", func(t *testing.T) {
		repo1, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})
		quarantine1, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo1), locator)
		require.NoError(t, err)

		repo2, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})
		quarantine2, err := quarantine.New(ctx, gittest.RewrittenRepository(t, ctx, cfg, repo2), locator)
		require.NoError(t, err)

		// We swap out the the object directories of both quarantines. So while both are
		// valid, we still expect that this RPC call fails because we detect that the
		// swapped-in quarantine directory does not belong to our repository.
		repo := proto.Clone(quarantine1.QuarantinedRepo()).(*gitalypb.Repository)
		repo.GitObjectDirectory = quarantine2.QuarantinedRepo().GetGitObjectDirectory()
		// quarantine.New in Gitaly would receive an already rewritten repository. Gitaly would then calculate
		// the quarantine directories based on the rewritten relative path. That quarantine would then be looped
		// through Rails, which would then send a request with the quarantine object directories set based on the
		// rewritten relative path but with the original relative path of the repository. Since we're using the production
		// helpers here, we need to manually substitute the rewritten relative path with the original one when sending
		// it back through the API.
		repo.RelativePath = repo1.RelativePath

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: repo,
		})
		require.Error(t, err, "rpc error: code = InvalidArgument desc = GetObjectDirectoryPath: relative path escapes root directory")
		require.Nil(t, response)
	})
}
