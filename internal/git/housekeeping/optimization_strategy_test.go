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
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestNewHeuristicalOptimizationStrategy_variousParameters(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc             string
		setup            func(t *testing.T, relativePath string) *gitalypb.Repository
		expectedStrategy HeuristicalOptimizationStrategy
	}{
		{
			desc: "empty repo",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{},
		},
		{
			desc: "object in 17 shard",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				looseObjectPath := filepath.Join(repoPath, "objects", "17", "1234")
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))
				require.NoError(t, os.WriteFile(looseObjectPath, nil, 0o644))

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 256,
			},
		},
		{
			desc: "packfile",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "pack", "pack-foobar.pack"), nil, 0o644))

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				packfileCount: 1,
			},
		},
		{
			desc: "alternate",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), nil, 0o644))

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				hasAlternate: true,
			},
		},
		{
			desc: "bitmap",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "pack", "pack-1234.bitmap"), nil, 0o644))

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				hasBitmap: true,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			testRepoAndPool(t, tc.desc, func(t *testing.T, relativePath string) {
				repoProto := tc.setup(t, relativePath)
				repo := localrepo.NewTestRepo(t, cfg, repoProto)

				tc.expectedStrategy.isObjectPool = IsPoolRepository(repo)

				strategy, err := NewHeuristicalOptimizationStrategy(ctx, repo)
				require.NoError(t, err)
				require.Equal(t, tc.expectedStrategy, strategy)
			})
		})
	}
}

func TestNewHeuristicalOptimizationStrategy_looseObjectCount(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc                     string
		looseObjects             []string
		expectedLooneObjectCount int64
	}{
		{
			desc:                     "no objects",
			looseObjects:             nil,
			expectedLooneObjectCount: 0,
		},
		{
			desc: "object not in 17 shard",
			looseObjects: []string{
				filepath.Join("ab/12345"),
			},
			expectedLooneObjectCount: 0,
		},
		{
			desc: "object in 17 shard",
			looseObjects: []string{
				filepath.Join("17/12345"),
			},
			expectedLooneObjectCount: 256,
		},
		{
			desc: "objects in different shards",
			looseObjects: []string{
				filepath.Join("ab/12345"),
				filepath.Join("cd/12345"),
				filepath.Join("12/12345"),
				filepath.Join("17/12345"),
			},
			expectedLooneObjectCount: 256,
		},
		{
			desc: "multiple objects in 17 shard",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
			},
			expectedLooneObjectCount: 1024,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, looseObjectPath := range tc.looseObjects {
				looseObjectPath := filepath.Join(repoPath, "objects", looseObjectPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))

				looseObjectFile, err := os.Create(looseObjectPath)
				require.NoError(t, err)
				testhelper.MustClose(t, looseObjectFile)
			}

			strategy, err := NewHeuristicalOptimizationStrategy(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, HeuristicalOptimizationStrategy{
				looseObjectCount: tc.expectedLooneObjectCount,
			}, strategy)
		})
	}
}

func TestHeuristicalOptimizationStrategy_ShouldRepackObjects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		strategy       HeuristicalOptimizationStrategy
		expectedNeeded bool
		expectedConfig RepackObjectsConfig
	}{
		{
			desc:     "empty repo does nothing",
			strategy: HeuristicalOptimizationStrategy{},
		},
		{
			desc: "missing bitmap",
			strategy: HeuristicalOptimizationStrategy{
				hasBitmap:     false,
				hasAlternate:  false,
				packfileCount: 1,
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			strategy: HeuristicalOptimizationStrategy{
				hasBitmap:     false,
				hasAlternate:  true,
				packfileCount: 1,
			},
			// If we have no bitmap in the repository we'd normally want to fully repack
			// the repository. But because we have an alternates file we know that the
			// repository must not have a bitmap anyway, so we can skip the repack here.
			expectedNeeded: false,
		},
		{
			desc: "no repack needed",
			strategy: HeuristicalOptimizationStrategy{
				hasBitmap:     true,
				packfileCount: 1,
			},
			expectedNeeded: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repackNeeded, repackCfg := tc.strategy.ShouldRepackObjects()
			require.Equal(t, tc.expectedNeeded, repackNeeded)
			require.Equal(t, tc.expectedConfig, repackCfg)
		})
	}

	for _, outerTC := range []struct {
		largestPackfileSizeInMB  int64
		requiredPackfiles        int64
		requiredPackfilesForPool int64
	}{
		{
			largestPackfileSizeInMB:  1,
			requiredPackfiles:        5,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  5,
			requiredPackfiles:        6,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  10,
			requiredPackfiles:        8,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  50,
			requiredPackfiles:        14,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  100,
			requiredPackfiles:        17,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  500,
			requiredPackfiles:        23,
			requiredPackfilesForPool: 2,
		},
		{
			largestPackfileSizeInMB:  1001,
			requiredPackfiles:        26,
			requiredPackfilesForPool: 3,
		},
	} {
		t.Run(fmt.Sprintf("packfile with %dMB", outerTC.largestPackfileSizeInMB), func(t *testing.T) {
			for _, tc := range []struct {
				desc              string
				isPool            bool
				hasAlternate      bool
				requiredPackfiles int64
			}{
				{
					desc:              "normal repository",
					isPool:            false,
					requiredPackfiles: outerTC.requiredPackfiles,
				},
				{
					desc:              "pooled repository",
					isPool:            false,
					hasAlternate:      true,
					requiredPackfiles: outerTC.requiredPackfiles,
				},
				{
					desc:              "object pool",
					isPool:            true,
					requiredPackfiles: outerTC.requiredPackfilesForPool,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					strategy := HeuristicalOptimizationStrategy{
						largestPackfileSizeInMB: outerTC.largestPackfileSizeInMB,
						packfileCount:           tc.requiredPackfiles - 1,
						isObjectPool:            tc.isPool,
						hasAlternate:            tc.hasAlternate,
						hasBitmap:               true,
					}

					repackNeeded, _ := strategy.ShouldRepackObjects()
					require.False(t, repackNeeded)

					// Now we add the last packfile that should bring us across
					// the boundary of having to repack.
					strategy.packfileCount++

					repackNeeded, repackCfg := strategy.ShouldRepackObjects()
					require.True(t, repackNeeded)
					require.Equal(t, RepackObjectsConfig{
						FullRepack:  true,
						WriteBitmap: !tc.hasAlternate,
					}, repackCfg)
				})
			}
		})
	}

	for _, outerTC := range []struct {
		desc           string
		looseObjects   int64
		expectedRepack bool
	}{
		{
			desc:           "no objects",
			looseObjects:   0,
			expectedRepack: false,
		},
		{
			desc:           "single object",
			looseObjects:   1,
			expectedRepack: false,
		},
		{
			desc:           "boundary",
			looseObjects:   1024,
			expectedRepack: false,
		},
		{
			desc:           "exceeding boundary should cause repack",
			looseObjects:   1025,
			expectedRepack: true,
		},
	} {
		for _, tc := range []struct {
			desc   string
			isPool bool
		}{
			{
				desc:   "normal repository",
				isPool: false,
			},
			{
				desc:   "object pool",
				isPool: true,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				strategy := HeuristicalOptimizationStrategy{
					looseObjectCount: outerTC.looseObjects,
					isObjectPool:     tc.isPool,
					// We need to pretend that we have a bitmap, otherwise we
					// aways do a full repack.
					hasBitmap: true,
				}

				repackNeeded, repackCfg := strategy.ShouldRepackObjects()
				require.Equal(t, outerTC.expectedRepack, repackNeeded)
				require.Equal(t, RepackObjectsConfig{
					FullRepack:  false,
					WriteBitmap: false,
				}, repackCfg)
			})
		}
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
type mockOptimizationStrategy struct {
	shouldRepackObjects bool
	repackObjectsCfg    RepackObjectsConfig
}

func (m mockOptimizationStrategy) ShouldRepackObjects() (bool, RepackObjectsConfig) {
	return m.shouldRepackObjects, m.repackObjectsCfg
}
