//go:build !gitaly_test_sha256

package repository

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Bring the repository into a known-good state with a single packfile, only.
	initialCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")
	oldPackfileCount, err := stats.PackfilesCount(repo)
	require.NoError(t, err)
	require.Equal(t, uint64(1), oldPackfileCount)

	// Write a second commit into the repository so that we have something to repack.
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(initialCommit), gittest.WithBranch("main"))

	//nolint:staticcheck
	c, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{
		Repository: repoProto,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// As we have done an incremental repack we expect to see one more packfile than before now.
	newPackfileCount, err := stats.PackfilesCount(repo)
	require.NoError(t, err)
	require.Equal(t, oldPackfileCount+1, newPackfileCount)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists:                 true,
		HasBloomFilters:        true,
		HasGenerationData:      true,
		CommitGraphChainLength: 1,
	})
}

func TestRepackIncrementalCollectLogStatistics(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	logger, hook := test.NewNullLogger()
	_, repo, _, client := setupRepositoryService(t, ctx, testserver.WithLogger(logger))

	//nolint:staticcheck
	_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)

	requireRepositoryInfoLog(t, hook.AllEntries()...)
}

func TestRepackLocal(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	altObjectsDir := "./alt-objects"
	alternateCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("alternate commit"),
		gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, altObjectsDir)),
		gittest.WithBranch("alternate-odb"),
	)

	repoCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("main commit"),
		gittest.WithBranch("main-odb"),
	)

	// Set GIT_ALTERNATE_OBJECT_DIRECTORIES on the outgoing request. The
	// intended use case of the behavior we're testing here is that
	// alternates are found through the objects/info/alternates file instead
	// of GIT_ALTERNATE_OBJECT_DIRECTORIES. But for the purpose of this test
	// it doesn't matter.
	repo.GitAlternateObjectDirectories = []string{altObjectsDir}
	//nolint:staticcheck
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1)

	packContents := gittest.Exec(t, cfg, "-C", repoPath, "verify-pack", "-v", packFiles[0])
	require.NotContains(t, string(packContents), alternateCommit.String())
	require.Contains(t, string(packContents), repoCommit.String())
}

const praefectErr = `routing repository maintenance: getting repository metadata: repository not found`

func TestRepackIncrementalFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		repo *gitalypb.Repository
		err  error
		desc string
	}{
		{
			desc: "nil repo",
			repo: nil,
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid storage name",
			repo: &gitalypb.Repository{RelativePath: "stub", StorageName: "foo"},
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`repacking objects: GetStorageByName: no such storage: "foo"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "no storage name",
			repo: &gitalypb.Repository{RelativePath: "bar"},
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty StorageName",
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "non-existing repo",
			repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"},
			err: status.Error(
				codes.NotFound,
				testhelper.GitalyOrPraefect(
					fmt.Sprintf(`repacking objects: GetRepoPath: not a git repository: "%s/bar"`, cfg.Storages[0].Path),
					praefectErr,
				),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: tc.repo})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}

func TestRepackFullSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc         string
		createBitmap bool
	}{
		{
			desc:         "with bitmap",
			createBitmap: true,
		},
		{
			desc:         "without bitmap",
			createBitmap: false,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			// Bring the repository into a known state with two packfiles.
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
			gittest.Exec(t, cfg, "-C", repoPath, "repack")
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))
			gittest.Exec(t, cfg, "-C", repoPath, "repack")
			oldPackfileCount, err := stats.PackfilesCount(repo)
			require.NoError(t, err)
			require.Equal(t, uint64(2), oldPackfileCount)

			//nolint:staticcheck
			response, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{
				Repository:   repoProto,
				CreateBitmap: tc.createBitmap,
			})
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.RepackFullResponse{}, response)

			// After the full repack we should see that all packfiles have been repacked
			// into a single one.
			newPackfileCount, err := stats.PackfilesCount(repo)
			require.NoError(t, err)
			require.Equal(t, uint64(1), newPackfileCount)

			// We should also see that the bitmap has been generated if requested.
			bitmaps, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "pack-*.bitmap"))
			require.NoError(t, err)
			if tc.createBitmap {
				require.Len(t, bitmaps, 1)

				bitmapInfo, err := stats.BitmapInfoForPath(bitmaps[0])
				require.NoError(t, err)
				require.Equal(t, stats.BitmapInfo{
					Exists:         true,
					Version:        1,
					HasHashCache:   true,
					HasLookupTable: true,
				}, bitmapInfo)
			} else {
				require.Empty(t, bitmaps)
			}

			// And last but not least the commit-graph must've been written. This is
			// important because the commit-graph might otherwise be stale.
			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists:                 true,
				HasBloomFilters:        true,
				HasGenerationData:      true,
				CommitGraphChainLength: 1,
			})
		})
	}
}

func TestRepackFullCollectLogStatistics(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	logger, hook := test.NewNullLogger()
	_, repo, _, client := setupRepositoryService(t, ctx, testserver.WithLogger(logger))

	//nolint:staticcheck
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	requireRepositoryInfoLog(t, hook.AllEntries()...)
}

func TestRepackFullFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		desc string
		repo *gitalypb.Repository
		err  error
	}{
		{
			desc: "nil repo",
			repo: nil,
			err:  status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect("empty Repository", "repo scoped: empty Repository")),
		},
		{
			desc: "invalid storage name",
			repo: &gitalypb.Repository{RelativePath: "stub", StorageName: "foo"},
			err:  status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(`repacking objects: GetStorageByName: no such storage: "foo"`, "repo scoped: invalid Repository")),
		},
		{
			desc: "no storage name",
			repo: &gitalypb.Repository{RelativePath: "bar"},
			err:  status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect("empty StorageName", "repo scoped: invalid Repository")),
		},
		{
			desc: "non-existing repo",
			repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"},
			err: status.Error(
				codes.NotFound,
				testhelper.GitalyOrPraefect(
					fmt.Sprintf(`repacking objects: GetRepoPath: not a git repository: "%s/bar"`, cfg.Storages[0].Path),
					praefectErr,
				),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: tc.repo})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}

func TestRepackFullDeltaIslands(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	gittest.TestDeltaIslands(t, cfg, repoPath, repoPath, false, func() error {
		//nolint:staticcheck
		_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
		return err
	})
}
