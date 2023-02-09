//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	offsetUntilOld         = -2 * time.Hour
	offsetUntilOldWorktree = -7 * time.Hour
)

func TestGarbageCollectCommitGraph(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	//nolint:staticcheck
	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	requireCommitGraphInfo(t, repoPath, stats.CommitGraphInfo{
		Exists:                 true,
		CommitGraphChainLength: 1,
		HasBloomFilters:        true,
		HasGenerationData:      true,
	})
}

func TestGarbageCollectSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	tests := []struct {
		req  *gitalypb.GarbageCollectRequest
		desc string
	}{
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: repo, CreateBitmap: false},
			desc: "without bitmap",
		},
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: repo, CreateBitmap: true},
			desc: "with bitmap",
		},
	}

	packPath := filepath.Join(repoPath, "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			require.NoError(t, os.Chtimes(packPath, testTime, testTime))
			//nolint:staticcheck
			c, err := client.GarbageCollect(ctx, test.req)
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
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}
		})
	}
}

func TestGarbageCollectWithPrune(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	blobHashes := gittest.WriteBlobs(t, cfg, repoPath, 3)
	oldDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[0][:2], blobHashes[0][2:])
	newDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[1][:2], blobHashes[1][2:])
	oldReferencedObjFile := filepath.Join(repoPath, "objects", blobHashes[2][:2], blobHashes[2][2:])

	// create a reference to the blob, so it should not be removed by gc
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{
			OID: git.ObjectID(blobHashes[2]), Path: "blob-name", Mode: "100644",
		}),
	)

	// change modification time of the blobs to make them attractive for the gc
	aBitMoreThan30MinutesAgo := time.Now().Add(-30*time.Minute - time.Second)
	farAgo := time.Date(2015, 1, 1, 1, 1, 1, 1, time.UTC)
	require.NoError(t, os.Chtimes(oldDanglingObjFile, aBitMoreThan30MinutesAgo, aBitMoreThan30MinutesAgo))
	require.NoError(t, os.Chtimes(newDanglingObjFile, time.Now(), time.Now()))
	require.NoError(t, os.Chtimes(oldReferencedObjFile, farAgo, farAgo))

	// Prune option has no effect when disabled
	//nolint:staticcheck
	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo, Prune: false})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.FileExists(t, oldDanglingObjFile, "blob should not be removed from object storage as it was modified less then 2 weeks ago")

	// Prune option has effect when enabled
	//nolint:staticcheck
	c, err = client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo, Prune: true})
	require.NoError(t, err)
	require.NotNil(t, c)

	require.NoFileExists(t, oldDanglingObjFile, "blob should be removed from object storage as it is too old and there are no references to it")
	require.FileExists(t, newDanglingObjFile, "blob should not be removed from object storage as it is fresh enough despite there are no references to it")
	require.FileExists(t, oldReferencedObjFile, "blob should not be removed from object storage as it is referenced by something despite it is too old")
}

func TestGarbageCollectLogStatistics(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	logger, hook := test.NewNullLogger()
	_, repo, _, client := setupRepositoryService(t, ctx, testserver.WithLogger(logger))

	//nolint:staticcheck
	_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
	require.NoError(t, err)

	requireRepositoryInfoLog(t, hook.AllEntries()...)
}

func TestGarbageCollectDeletesRefsLocks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	req := &gitalypb.GarbageCollectRequest{Repository: repo}
	refsPath := filepath.Join(repoPath, "refs")

	now := time.Now()
	old := now.Add(offsetUntilOld)

	// Note: Creating refs this way makes `git gc` crash but this actually works
	// in our favor for this test since we can ensure that the files kept and
	// deleted are all due to our *.lock cleanup step before gc runs (since
	// `git gc` also deletes files from /refs when packing).
	keepRefPath := filepath.Join(refsPath, "heads", "keepthis")
	mustCreateFileWithTimes(t, keepRefPath, now)
	keepOldRefPath := filepath.Join(refsPath, "heads", "keepthisalso")
	mustCreateFileWithTimes(t, keepOldRefPath, old)
	keepDeceitfulRef := filepath.Join(refsPath, "heads", " .lock.not-actually-a-lock.lock ")
	mustCreateFileWithTimes(t, keepDeceitfulRef, old)

	keepLockPath := filepath.Join(refsPath, "heads", "keepthis.lock")
	mustCreateFileWithTimes(t, keepLockPath, now)

	deleteLockPath := filepath.Join(refsPath, "heads", "deletethis.lock")
	mustCreateFileWithTimes(t, deleteLockPath, old)

	//nolint:staticcheck
	c, err := client.GarbageCollect(ctx, req)
	testhelper.RequireGrpcCode(t, err, codes.Internal)
	require.Contains(t, err.Error(), "cmd wait")
	assert.Nil(t, c)

	// Sanity checks
	assert.FileExists(t, keepRefPath)
	assert.FileExists(t, keepOldRefPath)
	assert.FileExists(t, keepDeceitfulRef)

	assert.FileExists(t, keepLockPath)

	require.NoFileExists(t, deleteLockPath)
}

func TestGarbageCollectDeletesPackedRefsLock(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc        string
		createLock  func(t *testing.T, lockfilePath string)
		shouldExist bool
	}{
		{
			desc: "with a recent lock",
			createLock: func(t *testing.T, lockfilePath string) {
				mustCreateFileWithTimes(t, lockfilePath, time.Now())
			},
			shouldExist: true,
		},
		{
			desc: "with an old lock",
			createLock: func(t *testing.T, lockfilePath string) {
				mustCreateFileWithTimes(t, lockfilePath, time.Now().Add(offsetUntilOld))
			},
			shouldExist: false,
		},
		{
			desc:        "with a non-existing lock",
			shouldExist: false,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

			// Force the packed-refs file to have an old time to test that even
			// in that case it doesn't get deleted
			packedRefsPath := filepath.Join(repoPath, "packed-refs")
			ancient := time.Now().Add(-7 * 24 * time.Hour)
			require.NoError(t, os.Chtimes(packedRefsPath, ancient, ancient))

			req := &gitalypb.GarbageCollectRequest{Repository: repo}
			lockPath := filepath.Join(repoPath, "packed-refs.lock")

			if tc.createLock != nil {
				tc.createLock(t, lockPath)
			}

			//nolint:staticcheck
			c, err := client.GarbageCollect(ctx, req)

			// Sanity checks
			assert.FileExists(t, filepath.Join(repoPath, "HEAD")) // For good measure
			assert.FileExists(t, packedRefsPath)

			if tc.shouldExist {
				assert.Error(t, err)
				testhelper.RequireGrpcCode(t, err, codes.Internal)

				require.FileExists(t, lockPath)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, c)

				require.NoFileExists(t, lockPath)
			}
		})
	}
}

func TestGarbageCollectDeletesFileLocks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc                string
		lockfile            string
		expectedErrContains string
	}{
		{
			desc:     "locked gitconfig",
			lockfile: "config.lock",
		},
		{
			desc:     "locked HEAD",
			lockfile: "HEAD.lock",
		},
		{
			desc:     "locked commit-graph",
			lockfile: "objects/info/commit-graphs/commit-graph-chain.lock",
			// Writing commit-graphs fails if there is another, concurrent process that
			// has locked the commit-graph chain.
			expectedErrContains: "Another git process seems to be running in this repository",
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Create a lockfile and run GarbageCollect. Because the lock has been
			// freshly created GarbageCollect shouldn't remove the not-yet-stale
			// lockfile.
			t.Run("with recent lockfile", func(t *testing.T) {
				t.Parallel()

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				lockPath := filepath.Join(repoPath, tc.lockfile)
				mustCreateFileWithTimes(t, lockPath, time.Now())

				//nolint:staticcheck
				_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{
					Repository: repo,
				})
				if tc.expectedErrContains == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectedErrContains)
				}
				require.FileExists(t, lockPath)
			})

			// Redo the same test, but this time we create the lockfile so that it is
			// considered stale. GarbageCollect should know to remove it.
			t.Run("with stale lockfile", func(t *testing.T) {
				t.Parallel()

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				lockPath := filepath.Join(repoPath, tc.lockfile)
				mustCreateFileWithTimes(t, lockPath, time.Now().Add(offsetUntilOld))

				//nolint:staticcheck
				_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{
					Repository: repo,
				})
				require.NoError(t, err)
				require.NoFileExists(t, lockPath)
			})
		})
	}
}

func TestGarbageCollectDeletesPackedRefsNew(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc           string
		createLockfile func(t *testing.T, lockfilePath string)
		shouldExist    bool
	}{
		{
			desc: "created recently",
			createLockfile: func(t *testing.T, lockfilePath string) {
				mustCreateFileWithTimes(t, lockfilePath, time.Now())
			},
			shouldExist: true,
		},
		{
			desc: "exists for too long",
			createLockfile: func(t *testing.T, lockfilePath string) {
				mustCreateFileWithTimes(t, lockfilePath, time.Now().Add(offsetUntilOld))
			},
			shouldExist: false,
		},
		{
			desc:        "nothing to clean up",
			shouldExist: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

			req := &gitalypb.GarbageCollectRequest{Repository: repo}
			packedRefsNewPath := filepath.Join(repoPath, "packed-refs.new")

			if tc.createLockfile != nil {
				tc.createLockfile(t, packedRefsNewPath)
			}

			//nolint:staticcheck
			c, err := client.GarbageCollect(ctx, req)

			if tc.shouldExist {
				require.Error(t, err)
				testhelper.RequireGrpcCode(t, err, codes.Internal)

				require.FileExists(t, packedRefsNewPath)
			} else {
				require.NotNil(t, c)
				require.NoError(t, err)

				require.NoFileExists(t, packedRefsNewPath)
			}
		})
	}
}

func TestGarbageCollectFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)
	storagePath := strings.TrimSuffix(repoPath, "/"+repo.RelativePath)

	tests := []struct {
		repo *gitalypb.Repository
		err  error
	}{
		{
			repo: nil,
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			repo: &gitalypb.Repository{RelativePath: "stub", StorageName: "foo"},
			err: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`GetStorageByName: no such storage: "foo"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			repo: &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "bar"},
			err: status.Error(
				codes.NotFound,
				testhelper.GitalyOrPraefect(
					fmt.Sprintf(`GetRepoPath: not a git repository: "%s/bar"`, storagePath),
					`routing repository maintenance: getting repository metadata: repository not found`,
				),
			),
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v", tc.repo), func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: tc.repo})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}

func TestCleanupInvalidKeepAroundRefs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	// Make the directory, so we can create random reflike things in it
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "keep-around"), perm.SharedDir))

	testCases := []struct {
		desc        string
		refName     string
		refContent  string
		shouldExist bool
	}{
		{
			desc:        "A valid ref",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			shouldExist: true,
		},
		{
			desc:        "A ref that does not exist",
			refName:     "bogus",
			refContent:  "bogus",
			shouldExist: false,
		},
		{
			desc:        "Filled with the blank ref",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  git.ObjectHashSHA1.ZeroOID.String(),
			shouldExist: true,
		},
		{
			desc:        "An existing ref with blank content",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  "",
			shouldExist: true,
		},
		{
			desc:        "A valid sha that does not exist in the repo",
			refName:     "d669a6f1a70693058cf484318c1cee8526119938",
			refContent:  "d669a6f1a70693058cf484318c1cee8526119938",
			shouldExist: false,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.desc, func(t *testing.T) {
			// Create a proper keep-around loose ref
			existingSha := "1e292f8fedd741b75372e19097c76d327140c312"
			existingRefName := fmt.Sprintf("refs/keep-around/%s", existingSha)
			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", existingRefName, existingSha)

			// Create an invalid ref that should should be removed with the testcase
			bogusSha := "b3f5e4adf6277b571b7943a4f0405a6dd7ee7e15"
			bogusPath := filepath.Join(repoPath, fmt.Sprintf("refs/keep-around/%s", bogusSha))
			require.NoError(t, os.WriteFile(bogusPath, []byte(bogusSha), perm.SharedFile))

			// Creating the keeparound without using git so we can create invalid ones in testcases
			refPath := filepath.Join(repoPath, fmt.Sprintf("refs/keep-around/%s", testcase.refName))
			require.NoError(t, os.WriteFile(refPath, []byte(testcase.refContent), perm.SharedFile))

			// Perform the request
			req := &gitalypb.GarbageCollectRequest{Repository: repo}
			//nolint:staticcheck
			_, err := client.GarbageCollect(ctx, req)
			require.NoError(t, err)

			// The existing keeparound still exists
			commitSha := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", existingRefName)
			require.Equal(t, existingSha, text.ChompBytes(commitSha))

			// The invalid one was removed
			require.NoFileExists(t, bogusPath)

			if testcase.shouldExist {
				keepAroundName := fmt.Sprintf("refs/keep-around/%s", testcase.refName)
				commitSha := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", keepAroundName)
				require.Equal(t, testcase.refName, text.ChompBytes(commitSha))
			} else {
				require.NoFileExists(t, refPath)
			}
		})
	}
}

func mustCreateFileWithTimes(tb testing.TB, path string, mTime time.Time) {
	tb.Helper()

	require.NoError(tb, os.MkdirAll(filepath.Dir(path), perm.SharedDir))
	require.NoError(tb, os.WriteFile(path, nil, perm.SharedFile))
	require.NoError(tb, os.Chtimes(path, mTime, mTime))
}

func TestGarbageCollectDeltaIslands(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	gittest.TestDeltaIslands(t, cfg, repoPath, repoPath, false, func() error {
		//nolint:staticcheck
		_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
		return err
	})
}

func TestGarbageCollect_commitGraphsWithPrunedObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// Write a first commit-graph that contains the root commit, only.
	rootCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split", "--changed-paths")

	// Write a second, incremental commit-graph that contains a commit we're about to
	// make unreachable and then prune.
	unreachableCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(rootCommitID), gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split=no-merge", "--changed-paths")

	// Reset the "main" branch back to the initial root commit ID and prune the now
	// unreachable second commit.
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/main", rootCommitID.String())
	gittest.Exec(t, cfg, "-C", repoPath, "prune", "--expire", "now")

	// The commit-graph chain now refers to the pruned commit, and git-commit-graph(1)
	// should complain about that.
	var stderr bytes.Buffer
	verifyCmd := gittest.NewCommand(t, cfg, "-C", repoPath, "commit-graph", "verify")
	verifyCmd.Stderr = &stderr
	require.EqualError(t, verifyCmd.Run(), "exit status 1")
	require.Equal(t, stderr.String(), fmt.Sprintf("error: Could not read %[1]s\nfailed to parse commit %[1]s from object database for commit-graph\n", unreachableCommitID))

	// Given that GarbageCollect is an RPC that prunes objects it should know to fix up commit
	// graphs...
	//nolint:staticcheck
	_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repoProto})
	require.NoError(t, err)

	// ... and it does.
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "verify")
}
