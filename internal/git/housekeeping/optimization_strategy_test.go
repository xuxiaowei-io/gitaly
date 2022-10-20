//go:build !gitaly_test_sha256

package housekeeping

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestNeedsRepacking(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T, relativePath string) *gitalypb.Repository
		expectedErr    error
		expectedNeeded bool
		expectedConfig RepackObjectsConfig
	}{
		{
			desc: "empty repo does nothing",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				return repoProto
			},
		},
		{
			desc: "missing bitmap",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					Seed:                   gittest.SeedGitLabTest,
					RelativePath:           relativePath,
				})
				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					Seed:                   gittest.SeedGitLabTest,
					RelativePath:           relativePath,
				})

				// Create the alternates file. If it exists, then we shouldn't try
				// to generate a bitmap.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), nil, 0o755))

				return repoProto
			},
			// If we have no bitmap in the repository we'd normally want to fully repack
			// the repository. But because we have an alternates file we know that the
			// repository must not have a bitmap anyway, so we can skip the repack here.
			expectedNeeded: false,
		},
		{
			desc: "missing commit-graph",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					Seed:                   gittest.SeedGitLabTest,
					RelativePath:           relativePath,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")

				return repoProto
			},
			// The commit-graph used to influence whether we repacked a repository or
			// not. That was due to historic reasons only, though, and ultimately does
			// not make any sense.
			expectedNeeded: false,
		},
		{
			desc: "commit-graph without bloom filters",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					Seed:                   gittest.SeedGitLabTest,
					RelativePath:           relativePath,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

				return repoProto
			},
			// The commit-graph used to influence whether we repacked a repository or
			// not. That was due to historic reasons only, though, and ultimately does
			// not make any sense.
			expectedNeeded: false,
		},
		{
			desc: "no repack needed",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					Seed:                   gittest.SeedGitLabTest,
					RelativePath:           relativePath,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--changed-paths", "--split")

				return repoProto
			},
			expectedNeeded: false,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			testRepoAndPool(t, tc.desc, func(t *testing.T, relativePath string) {
				repoProto := tc.setup(t, relativePath)
				repo := localrepo.NewTestRepo(t, cfg, repoProto)

				repackNeeded, repackCfg, err := needsRepacking(repo)
				require.Equal(t, tc.expectedErr, err)
				require.Equal(t, tc.expectedNeeded, repackNeeded)
				require.Equal(t, tc.expectedConfig, repackCfg)
			})
		})
	}

	const megaByte = 1024 * 1024

	for _, tc := range []struct {
		packfileSize             int64
		requiredPackfiles        int
		requiredPackfilesForPool int
	}{
		{
			packfileSize:             1,
			requiredPackfiles:        5,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             5 * megaByte,
			requiredPackfiles:        6,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             10 * megaByte,
			requiredPackfiles:        8,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             50 * megaByte,
			requiredPackfiles:        14,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             100 * megaByte,
			requiredPackfiles:        17,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             500 * megaByte,
			requiredPackfiles:        23,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSize:             1001 * megaByte,
			requiredPackfiles:        26,
			requiredPackfilesForPool: 3,
		},
		// Let's not go any further than this, we're thrashing the temporary directory.
	} {
		testRepoAndPool(t, fmt.Sprintf("packfile with %d bytes", tc.packfileSize), func(t *testing.T, relativePath string) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           relativePath,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			packDir := filepath.Join(repoPath, "objects", "pack")

			// Emulate the existence of a bitmap and a commit-graph with bloom filters.
			// We explicitly don't want to generate them via Git commands as they would
			// require us to already have objects in the repository, and we want to be
			// in full control over all objects and packfiles in the repo.
			require.NoError(t, os.WriteFile(filepath.Join(packDir, "something.bitmap"), nil, 0o644))
			commitGraphChainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(commitGraphChainPath), 0o755))
			require.NoError(t, os.WriteFile(commitGraphChainPath, nil, 0o644))

			// We first create a single big packfile which is used to determine the
			// boundary of when we repack.
			bigPackPath := filepath.Join(packDir, "big.pack")
			require.NoError(t, os.WriteFile(bigPackPath, nil, 0o644))
			require.NoError(t, os.Truncate(bigPackPath, tc.packfileSize))

			requiredPackfiles := tc.requiredPackfiles
			if IsPoolRepository(repoProto) {
				requiredPackfiles = tc.requiredPackfilesForPool
			}

			// And then we create one less packfile than we need to hit the boundary.
			// This is done to assert that we indeed don't repack before hitting the
			// boundary.
			for i := 0; i < requiredPackfiles-2; i++ {
				additionalPackfile, err := os.Create(filepath.Join(packDir, fmt.Sprintf("%d.pack", i)))
				require.NoError(t, err)
				testhelper.MustClose(t, additionalPackfile)
			}

			repackNeeded, _, err := needsRepacking(repo)
			require.NoError(t, err)
			require.False(t, repackNeeded)

			// Now we create the additional packfile that causes us to hit the boundary.
			// We should thus see that we want to repack now.
			lastPackfile, err := os.Create(filepath.Join(packDir, "last.pack"))
			require.NoError(t, err)
			testhelper.MustClose(t, lastPackfile)

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.NoError(t, err)
			require.True(t, repackNeeded)
			require.Equal(t, RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			}, repackCfg)
		})
	}

	for _, tc := range []struct {
		desc           string
		looseObjects   []string
		expectedRepack bool
	}{
		{
			desc:           "no objects",
			looseObjects:   nil,
			expectedRepack: false,
		},
		{
			desc: "object not in 17 shard",
			looseObjects: []string{
				filepath.Join("ab/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "object in 17 shard",
			looseObjects: []string{
				filepath.Join("17/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "objects in different shards",
			looseObjects: []string{
				filepath.Join("ab/12345"),
				filepath.Join("cd/12345"),
				filepath.Join("12/12345"),
				filepath.Join("17/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "boundary",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
			},
			expectedRepack: false,
		},
		{
			desc: "exceeding boundary should cause repack",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
				filepath.Join("17/5"),
			},
			expectedRepack: true,
		},
	} {
		testRepoAndPool(t, tc.desc, func(t *testing.T, relativePath string) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           relativePath,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			// Emulate the existence of a bitmap and a commit-graph with bloom filters.
			// We explicitly don't want to generate them via Git commands as they would
			// require us to already have objects in the repository, and we want to be
			// in full control over all objects and packfiles in the repo.
			require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "pack", "something.bitmap"), nil, 0o644))
			commitGraphChainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(commitGraphChainPath), 0o755))
			require.NoError(t, os.WriteFile(commitGraphChainPath, nil, 0o644))

			for _, looseObjectPath := range tc.looseObjects {
				looseObjectPath := filepath.Join(repoPath, "objects", looseObjectPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))

				looseObjectFile, err := os.Create(looseObjectPath)
				require.NoError(t, err)
				testhelper.MustClose(t, looseObjectFile)
			}

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRepack, repackNeeded)
			if tc.expectedRepack {
				require.Equal(t, RepackObjectsConfig{
					FullRepack:  false,
					WriteBitmap: false,
				}, repackCfg)
			}
		})
	}
}

func TestEstimateLooseObjectCount(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("empty repository", func(t *testing.T) {
		looseObjects, err := estimateLooseObjectCount(repo, time.Now())
		require.NoError(t, err)
		require.Zero(t, looseObjects)
	})

	t.Run("object in different shard", func(t *testing.T) {
		differentShard := filepath.Join(repoPath, "objects", "a0")
		require.NoError(t, os.MkdirAll(differentShard, 0o755))

		object, err := os.Create(filepath.Join(differentShard, "123456"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err := estimateLooseObjectCount(repo, time.Now())
		require.NoError(t, err)
		require.Zero(t, looseObjects)
	})

	t.Run("object in estimation shard", func(t *testing.T) {
		estimationShard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(estimationShard, 0o755))

		object, err := os.Create(filepath.Join(estimationShard, "123456"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err := estimateLooseObjectCount(repo, time.Now())
		require.NoError(t, err)
		require.Equal(t, int64(256), looseObjects)

		// Create a second object in there.
		object, err = os.Create(filepath.Join(estimationShard, "654321"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err = estimateLooseObjectCount(repo, time.Now())
		require.NoError(t, err)
		require.Equal(t, int64(512), looseObjects)
	})

	t.Run("object in estimation shard with grace period", func(t *testing.T) {
		estimationShard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(estimationShard, 0o755))

		objectPaths := []string{
			filepath.Join(estimationShard, "123456"),
			filepath.Join(estimationShard, "654321"),
		}

		cutoffDate := time.Now()
		afterCutoffDate := cutoffDate.Add(1 * time.Minute)
		beforeCutoffDate := cutoffDate.Add(-1 * time.Minute)

		for _, objectPath := range objectPaths {
			require.NoError(t, os.WriteFile(objectPath, nil, 0o644))
			require.NoError(t, os.Chtimes(objectPath, afterCutoffDate, afterCutoffDate))
		}

		// Objects are recent, so with the cutoff-date they shouldn't be counted.
		looseObjects, err := estimateLooseObjectCount(repo, cutoffDate)
		require.NoError(t, err)
		require.Equal(t, int64(0), looseObjects)

		for i, objectPath := range objectPaths {
			// Modify the object's mtime should cause it to be counted.
			require.NoError(t, os.Chtimes(objectPath, beforeCutoffDate, beforeCutoffDate))

			looseObjects, err = estimateLooseObjectCount(repo, cutoffDate)
			require.NoError(t, err)
			require.Equal(t, int64((i+1)*256), looseObjects)
		}
	})
}

// mockOptimizationStrategy is a mock strategy that can be used with OptimizeRepository.
type mockOptimizationStrategy struct{}
