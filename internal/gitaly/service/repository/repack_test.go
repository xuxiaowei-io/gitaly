//go:build !gitaly_test_sha256

package repository

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
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
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	packPath := filepath.Join(repoPath, "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, filepath.Join(packPath, "*"))
	testTime := time.Date(2006, 0o1, 0o2, 15, 0o4, 0o5, 0, time.UTC)
	//nolint:staticcheck
	c, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists: true, HasBloomFilters: true, CommitGraphChainLength: 1,
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

	tests := []struct {
		req  *gitalypb.RepackFullRequest
		desc string
	}{
		{req: &gitalypb.RepackFullRequest{CreateBitmap: true}, desc: "with bitmap"},
		{req: &gitalypb.RepackFullRequest{CreateBitmap: false}, desc: "without bitmap"},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			var repoPath string
			test.req.Repository, repoPath = gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			packPath := filepath.Join(repoPath, "objects", "pack")
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			testTime := time.Date(2006, 0o1, 0o2, 15, 0o4, 0o5, 0, time.UTC)
			//nolint:staticcheck
			c, err := client.RepackFull(ctx, test.req)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			// Entire `path`-folder gets updated so this is fine :D
			assertModTimeAfter(t, testTime, packPath)

			bmPath, err := filepath.Glob(filepath.Join(packPath, "pack-*.bitmap"))
			if err != nil {
				t.Fatalf("Error globbing bitmaps: %v", err)
			}
			if test.req.GetCreateBitmap() {
				if len(bmPath) == 0 {
					t.Errorf("No bitmaps found")
				}
				doBitmapsContainHashCache(t, bmPath)
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}

			requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
				Exists: true, HasBloomFilters: true, CommitGraphChainLength: 1,
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

func doBitmapsContainHashCache(t *testing.T, bitmapPaths []string) {
	// for each bitmap file, check the 2-byte flag as documented in
	// https://github.com/git/git/blob/master/Documentation/technical/bitmap-format.txt
	for _, bitmapPath := range bitmapPaths {
		gittest.TestBitmapHasHashcache(t, bitmapPath)
	}
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
