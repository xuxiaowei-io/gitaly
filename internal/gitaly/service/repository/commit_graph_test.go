//go:build !gitaly_test_sha256

package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithDefaults(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testWriteCommitGraphWithExistingCommitGraphCreatedWithDefaults)
}

func testWriteCommitGraphWithExistingCommitGraphCreatedWithDefaults(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc               string
		commitGraphVersion string
	}{
		{
			desc:               "without preexisting generation data",
			commitGraphVersion: "1",
		},
		{
			desc:               "with preexisting generation data",
			commitGraphVersion: "2",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{})

			// write commit graph using an old approach
			gittest.Exec(t, cfg, "-C", repoPath,
				"-c", "commitGraph.generationVersion="+tc.commitGraphVersion,
				"commit-graph", "write", "--reachable",
			)
			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists:            true,
				HasGenerationData: tc.commitGraphVersion == "2",
			})
			treeEntry := localrepo.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
			localrepo.WriteTestCommit(t, localrepo.NewTestRepo(t, cfg, repo),
				localrepo.WithBranch(t.Name()),
				localrepo.WithTreeEntries(treeEntry))
			//nolint:staticcheck
			res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
				Repository:    repo,
				SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
			})
			require.NoError(t, err)
			require.NotNil(t, res)

			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists:                 true,
				HasBloomFilters:        true,
				HasGenerationData:      featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
				CommitGraphChainLength: 1,
			})
		})
	}
}

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithSplit(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testWriteCommitGraphWithExistingCommitGraphCreatedWithSplit)
}

func testWriteCommitGraphWithExistingCommitGraphCreatedWithSplit(t *testing.T, ctx context.Context) {
	t.Parallel()

	for _, tc := range []struct {
		desc               string
		commitGraphVersion string
	}{
		{
			desc:               "without preexisting generation data",
			commitGraphVersion: "1",
		},
		{
			desc:               "with preexisting generation data",
			commitGraphVersion: "2",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

			// Assert that no commit-graph exists.
			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{})

			// write commit graph chain
			gittest.Exec(t, cfg, "-C", repoPath,
				"-c", "commitGraph.generationVersion="+tc.commitGraphVersion,
				"commit-graph", "write", "--reachable", "--split",
			)
			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists:                 true,
				CommitGraphChainLength: 1,
				HasGenerationData:      tc.commitGraphVersion == "2",
			})

			treeEntry := localrepo.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
			localrepo.WriteTestCommit(t, localrepo.NewTestRepo(t, cfg, repo),
				localrepo.WithBranch(t.Name()),
				localrepo.WithTreeEntries(treeEntry))

			//nolint:staticcheck
			res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
				Repository:    repo,
				SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
			})
			require.NoError(t, err)
			require.NotNil(t, res)

			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists:                 true,
				CommitGraphChainLength: 1,
				HasBloomFilters:        true,
				HasGenerationData:      featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
			})
		})
	}
}

func TestWriteCommitGraph(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testWriteCommitGraph)
}

func testWriteCommitGraph(t *testing.T, ctx context.Context) {
	t.Parallel()

	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{})

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists:                 true,
		CommitGraphChainLength: 1,
		HasBloomFilters:        true,
		HasGenerationData:      featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
	})
}

func TestWriteCommitGraph_validationChecks(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testWriteCommitGraphValidationChecks)
}

func testWriteCommitGraphValidationChecks(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg, repo, _, client := setupRepositoryService(t, ctx)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.WriteCommitGraphRequest
		expectedErr error
	}{
		{
			desc: "invalid split strategy",
			req: &gitalypb.WriteCommitGraphRequest{
				Repository:    repo,
				SplitStrategy: gitalypb.WriteCommitGraphRequest_SplitStrategy(42),
			},
			expectedErr: status.Error(codes.InvalidArgument, "unsupported split strategy: 42"),
		},
		{
			desc: "no repository",
			req:  &gitalypb.WriteCommitGraphRequest{},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid storage",
			req:  &gitalypb.WriteCommitGraphRequest{Repository: &gitalypb.Repository{RelativePath: "stub", StorageName: "invalid"}},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`getting commit-graph config: GetStorageByName: no such storage: "invalid"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "not existing repository",
			req:  &gitalypb.WriteCommitGraphRequest{Repository: &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "invalid"}},
			expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefect(
				fmt.Sprintf(`getting commit-graph config: GetRepoPath: not a git repository: "%s/invalid"`, cfg.Storages[0].Path),
				"routing repository maintenance: getting repository metadata: repository not found",
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.WriteCommitGraph(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestUpdateCommitGraph(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.UseCommitGraphGenerationData).Run(t, testUpdateCommitGraph)
}

func testUpdateCommitGraph(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{})

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists:                 true,
		HasBloomFilters:        true,
		HasGenerationData:      featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
		CommitGraphChainLength: 1,
	})

	treeEntry := localrepo.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	localrepo.WriteTestCommit(t, localrepo.NewTestRepo(t, cfg, repo),
		localrepo.WithBranch(t.Name()),
		localrepo.WithTreeEntries(treeEntry))

	//nolint:staticcheck
	res, err = client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists:                 true,
		HasBloomFilters:        true,
		HasGenerationData:      featureflag.UseCommitGraphGenerationData.IsEnabled(ctx),
		CommitGraphChainLength: 2,
	})
}
