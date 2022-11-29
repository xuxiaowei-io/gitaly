//go:build !gitaly_test_sha256

package housekeeping

import (
	"fmt"
	"math"
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
				looseObjectCount: 1,
			},
		},
		{
			desc: "old object in 17 shard",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				shard := filepath.Join(repoPath, "objects", "17")
				require.NoError(t, os.MkdirAll(shard, 0o755))

				// We write one recent...
				require.NoError(t, os.WriteFile(filepath.Join(shard, "1234"), nil, 0o644))

				// ... and one stale object.
				staleObjectPath := filepath.Join(shard, "5678")
				require.NoError(t, os.WriteFile(staleObjectPath, nil, 0o644))
				twoWeeksAgo := time.Now().Add(CutOffTime)
				require.NoError(t, os.Chtimes(staleObjectPath, twoWeeksAgo, twoWeeksAgo))

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				looseObjectCount:    2,
				oldLooseObjectCount: 1,
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
		{
			desc: "existing unsplit commit-graph with bloom filters",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// Write a non-split commit-graph with bloom filters. We should
				// always rewrite the commit-graphs when we're not using a split
				// commit-graph. We make sure to add bloom filters via
				// `--changed-paths` given that it would otherwise cause us to
				// rewrite the graph regardless of whether the graph is split or not
				// if they were missing.
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--changed-paths")

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 2,
				looseRefsCount:   1,
			},
		},
		{
			desc: "existing split commit-graph without bloom filters",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// Generate a split commit-graph, but don't enable computation of
				// changed paths. This should trigger a rewrite so that we can
				// recompute all graphs and generate the changed paths.
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split")

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 2,
				looseRefsCount:   1,
			},
		},
		{
			desc: "existing split commit-graph with bloom filters",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				// Write a split commit-graph with bitmaps. This is the state we
				// want to be in, so there is no write required if we didn't also
				// repack objects.
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split", "--changed-paths")

				return repoProto
			},
			expectedStrategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 2,
				looseRefsCount:   1,
				hasBloomFilters:  true,
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
		expectedLooseObjectCount uint64
	}{
		{
			desc:                     "no objects",
			looseObjects:             nil,
			expectedLooseObjectCount: 0,
		},
		{
			desc: "object not in 17 shard",
			looseObjects: []string{
				filepath.Join("ab/12345"),
			},
			expectedLooseObjectCount: 1,
		},
		{
			desc: "object in 17 shard",
			looseObjects: []string{
				filepath.Join("17/12345"),
			},
			expectedLooseObjectCount: 1,
		},
		{
			desc: "objects in different shards",
			looseObjects: []string{
				filepath.Join("ab/12345"),
				filepath.Join("cd/12345"),
				filepath.Join("12/12345"),
				filepath.Join("17/12345"),
			},
			expectedLooseObjectCount: 4,
		},
		{
			desc: "multiple objects in 17 shard",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
			},
			expectedLooseObjectCount: 4,
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
				looseObjectCount: tc.expectedLooseObjectCount,
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
		packfileSizeInMB         uint64
		requiredPackfiles        uint64
		requiredPackfilesForPool uint64
	}{
		{
			packfileSizeInMB:         1,
			requiredPackfiles:        5,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         5,
			requiredPackfiles:        6,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         10,
			requiredPackfiles:        8,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         50,
			requiredPackfiles:        14,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         100,
			requiredPackfiles:        17,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         500,
			requiredPackfiles:        23,
			requiredPackfilesForPool: 2,
		},
		{
			packfileSizeInMB:         1001,
			requiredPackfiles:        26,
			requiredPackfilesForPool: 3,
		},
	} {
		t.Run(fmt.Sprintf("packfile with %dMB", outerTC.packfileSizeInMB), func(t *testing.T) {
			for _, tc := range []struct {
				desc              string
				isPool            bool
				hasAlternate      bool
				requiredPackfiles uint64
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
						packfileSize:  outerTC.packfileSizeInMB * 1024 * 1024,
						packfileCount: tc.requiredPackfiles - 1,
						isObjectPool:  tc.isPool,
						hasAlternate:  tc.hasAlternate,
						hasBitmap:     true,
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
		looseObjects   uint64
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

func TestHeuristicalOptimizationStrategy_ShouldPruneObjects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc                       string
		strategy                   HeuristicalOptimizationStrategy
		expectedShouldPruneObjects bool
	}{
		{
			desc:                       "empty repository",
			strategy:                   HeuristicalOptimizationStrategy{},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "only recent object",
			strategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 10000,
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "few stale objects",
			strategy: HeuristicalOptimizationStrategy{
				oldLooseObjectCount: 1000,
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "too many stale objects",
			strategy: HeuristicalOptimizationStrategy{
				oldLooseObjectCount: 1025,
			},
			expectedShouldPruneObjects: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("normal repository", func(t *testing.T) {
				require.Equal(t, tc.expectedShouldPruneObjects, tc.strategy.ShouldPruneObjects())
			})

			t.Run("object pool", func(t *testing.T) {
				strategy := tc.strategy
				strategy.isObjectPool = true
				require.False(t, strategy.ShouldPruneObjects())
			})
		})
	}
}

func TestHeuristicalOptimizationStrategy_ShouldRepackReferences(t *testing.T) {
	t.Parallel()

	const kiloByte = 1024

	for _, tc := range []struct {
		packedRefsSize int64
		requiredRefs   int64
	}{
		{
			packedRefsSize: 1,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 1 * kiloByte,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 10 * kiloByte,
			requiredRefs:   33,
		},
		{
			packedRefsSize: 100 * kiloByte,
			requiredRefs:   49,
		},
		{
			packedRefsSize: 1000 * kiloByte,
			requiredRefs:   66,
		},
		{
			packedRefsSize: 10000 * kiloByte,
			requiredRefs:   82,
		},
		{
			packedRefsSize: 100000 * kiloByte,
			requiredRefs:   99,
		},
	} {
		t.Run("packed-refs with %d bytes", func(t *testing.T) {
			strategy := HeuristicalOptimizationStrategy{
				packedRefsSize: tc.packedRefsSize,
				looseRefsCount: tc.requiredRefs - 1,
			}

			require.False(t, strategy.ShouldRepackReferences())

			strategy.looseRefsCount++

			require.True(t, strategy.ShouldRepackReferences())
		})
	}
}

func TestHeuristicalOptimizationStrategy_NeedsWriteCommitGraph(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc           string
		strategy       HeuristicalOptimizationStrategy
		expectedNeeded bool
		expectedCfg    WriteCommitGraphConfig
	}{
		{
			desc:           "empty repository",
			expectedNeeded: false,
		},
		{
			desc: "repository with objects but no refs",
			strategy: HeuristicalOptimizationStrategy{
				looseObjectCount: 9000,
			},
			expectedNeeded: false,
		},
		{
			desc: "repository without bloom filters",
			strategy: HeuristicalOptimizationStrategy{
				looseRefsCount: 1,
			},
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with split commit-graph with bitmap without repack",
			strategy: HeuristicalOptimizationStrategy{
				looseRefsCount:  1,
				hasBloomFilters: true,
			},
			// We use the information about whether we repacked objects as an indicator
			// whether something has changed in the repository. If it didn't, then we
			// assume no new objects exist and thus we don't rewrite the commit-graph.
			expectedNeeded: false,
		},
		{
			desc: "repository with split commit-graph with bitmap with repack",
			strategy: HeuristicalOptimizationStrategy{
				looseRefsCount:   1,
				hasBloomFilters:  true,
				looseObjectCount: 9000,
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
		},
		{
			desc: "repository with split commit-graph with bitmap with pruned objects",
			strategy: HeuristicalOptimizationStrategy{
				looseRefsCount:      1,
				hasBloomFilters:     true,
				oldLooseObjectCount: 9000,
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			needed, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph()
			require.Equal(t, tc.expectedNeeded, needed)
			require.Equal(t, tc.expectedCfg, writeCommitGraphCfg)
		})
	}
}

func TestCountLooseObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	createRepo := func(t *testing.T) (*localrepo.Repo, string) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return localrepo.NewTestRepo(t, cfg, repoProto), repoPath
	}

	t.Run("empty repository", func(t *testing.T) {
		repo, _ := createRepo(t)

		looseObjects, err := countLooseObjects(repo, time.Now())
		require.NoError(t, err)
		require.Zero(t, looseObjects)
	})

	t.Run("object in random shard", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		differentShard := filepath.Join(repoPath, "objects", "a0")
		require.NoError(t, os.MkdirAll(differentShard, 0o755))

		object, err := os.Create(filepath.Join(differentShard, "123456"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err := countLooseObjects(repo, time.Now())
		require.NoError(t, err)
		require.EqualValues(t, 1, looseObjects)
	})

	t.Run("objects in multiple shards", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		for _, shard := range []string{"00", "17", "32", "ff"} {
			shardPath := filepath.Join(repoPath, "objects", shard)
			require.NoError(t, os.MkdirAll(shardPath, 0o755))

			object, err := os.Create(filepath.Join(shardPath, "123456"))
			require.NoError(t, err)
			testhelper.MustClose(t, object)
		}

		looseObjects, err := countLooseObjects(repo, time.Now())
		require.NoError(t, err)
		require.EqualValues(t, 4, looseObjects)
	})

	t.Run("object in shard with grace period", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		shard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(shard, 0o755))

		objectPaths := []string{
			filepath.Join(shard, "123456"),
			filepath.Join(shard, "654321"),
		}

		cutoffDate := time.Now()
		afterCutoffDate := cutoffDate.Add(1 * time.Minute)
		beforeCutoffDate := cutoffDate.Add(-1 * time.Minute)

		for _, objectPath := range objectPaths {
			require.NoError(t, os.WriteFile(objectPath, nil, 0o644))
			require.NoError(t, os.Chtimes(objectPath, afterCutoffDate, afterCutoffDate))
		}

		// Objects are recent, so with the cutoff-date they shouldn't be counted.
		looseObjects, err := countLooseObjects(repo, cutoffDate)
		require.NoError(t, err)
		require.EqualValues(t, 0, looseObjects)

		for i, objectPath := range objectPaths {
			// Modify the object's mtime should cause it to be counted.
			require.NoError(t, os.Chtimes(objectPath, beforeCutoffDate, beforeCutoffDate))

			looseObjects, err = countLooseObjects(repo, cutoffDate)
			require.NoError(t, err)
			require.EqualValues(t, i+1, looseObjects)
		}
	})

	t.Run("shard with garbage", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		shard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(shard, 0o755))

		for _, objectName := range []string{"garbage", "012345"} {
			require.NoError(t, os.WriteFile(filepath.Join(shard, objectName), nil, 0o644))
		}

		looseObjects, err := countLooseObjects(repo, time.Now())
		require.NoError(t, err)
		require.EqualValues(t, 1, looseObjects)
	})
}

func BenchmarkCountLooseObjects(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg := testcfg.Build(b)

	createRepo := func(b *testing.B) (*localrepo.Repo, string) {
		repoProto, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return localrepo.NewTestRepo(b, cfg, repoProto), repoPath
	}

	b.Run("empty repository", func(b *testing.B) {
		repo, _ := createRepo(b)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := countLooseObjects(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with single object", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		objectPath := filepath.Join(repoPath, "objects", "17", "12345")
		require.NoError(b, os.Mkdir(filepath.Dir(objectPath), 0o755))
		require.NoError(b, os.WriteFile(objectPath, nil, 0o644))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := countLooseObjects(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with single object in each shard", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		for i := 0; i < 256; i++ {
			objectPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i), "12345")
			require.NoError(b, os.Mkdir(filepath.Dir(objectPath), 0o755))
			require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := countLooseObjects(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository hitting loose object limit", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		// Usually we shouldn't have a lot more than `looseObjectCount` objects in the
		// repository because we'd repack as soon as we hit that limit. So this benchmark
		// case tries to estimate the usual upper limit for loose objects we'd typically
		// have.
		looseObjectCount := int(math.Ceil(looseObjectLimit / 256))

		for i := 0; i < 256; i++ {
			shardPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i))
			require.NoError(b, os.Mkdir(shardPath, 0o755))

			for j := 0; j < looseObjectCount; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := countLooseObjects(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with lots of objects", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		for i := 0; i < 256; i++ {
			shardPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i))
			require.NoError(b, os.Mkdir(shardPath, 0o755))

			for j := 0; j < 1000; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := countLooseObjects(repo, time.Now())
			require.NoError(b, err)
		}
	})
}

func TestNewEagerOptimizationStrategy(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc             string
		setup            func(t *testing.T, relativePath string) *gitalypb.Repository
		expectedStrategy EagerOptimizationStrategy
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
			expectedStrategy: EagerOptimizationStrategy{},
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
			expectedStrategy: EagerOptimizationStrategy{
				hasAlternate: true,
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

				strategy, err := NewEagerOptimizationStrategy(ctx, repo)
				require.NoError(t, err)
				require.Equal(t, tc.expectedStrategy, strategy)
			})
		})
	}
}

func TestEagerOptimizationStrategy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc                     string
		strategy                 EagerOptimizationStrategy
		expectWriteBitmap        bool
		expectShouldPruneObjects bool
	}{
		{
			desc:                     "no alternate",
			expectWriteBitmap:        true,
			expectShouldPruneObjects: true,
		},
		{
			desc: "alternate",
			strategy: EagerOptimizationStrategy{
				hasAlternate: true,
			},
			expectShouldPruneObjects: true,
		},
		{
			desc: "object pool",
			strategy: EagerOptimizationStrategy{
				isObjectPool: true,
			},
			expectWriteBitmap: true,
		},
		{
			desc: "object pool with alternate",
			strategy: EagerOptimizationStrategy{
				hasAlternate: true,
				isObjectPool: true,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			shouldRepackObjects, repackObjectsCfg := tc.strategy.ShouldRepackObjects()
			require.True(t, shouldRepackObjects)
			require.Equal(t, RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: tc.expectWriteBitmap,
			}, repackObjectsCfg)

			shouldWriteCommitGraph, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph()
			require.True(t, shouldWriteCommitGraph)
			require.Equal(t, WriteCommitGraphConfig{
				ReplaceChain: true,
			}, writeCommitGraphCfg)

			require.Equal(t, tc.expectShouldPruneObjects, tc.strategy.ShouldPruneObjects())
			require.True(t, tc.strategy.ShouldRepackReferences())
		})
	}
}

// mockOptimizationStrategy is a mock strategy that can be used with OptimizeRepository.
type mockOptimizationStrategy struct {
	shouldRepackObjects    bool
	repackObjectsCfg       RepackObjectsConfig
	shouldPruneObjects     bool
	shouldRepackReferences bool
	shouldWriteCommitGraph bool
	writeCommitGraphCfg    WriteCommitGraphConfig
}

func (m mockOptimizationStrategy) ShouldRepackObjects() (bool, RepackObjectsConfig) {
	return m.shouldRepackObjects, m.repackObjectsCfg
}

func (m mockOptimizationStrategy) ShouldPruneObjects() bool {
	return m.shouldPruneObjects
}

func (m mockOptimizationStrategy) ShouldRepackReferences() bool {
	return m.shouldRepackReferences
}

func (m mockOptimizationStrategy) ShouldWriteCommitGraph() (bool, WriteCommitGraphConfig) {
	return m.shouldWriteCommitGraph, m.writeCommitGraphCfg
}
