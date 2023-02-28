package housekeeping

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestHeuristicalOptimizationStrategy_ShouldRepackObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

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
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: false,
						},
					},
					Alternates: []string{},
				},
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:          false,
				WriteBitmap:         true,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: false,
						},
					},
					Alternates: []string{"something"},
				},
			},
			// If we have no bitmap in the repository we'd normally want to fully repack
			// the repository. But because we have an alternates file we know that the
			// repository must not have a bitmap anyway, so we can skip the repack here.
			//
			// This changes though with multi-pack-indices, which allow for bitmaps to
			// exist in pooled repositories.
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "no repack needed without multi-pack-index",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count: 1,
						Bitmap: stats.BitmapInfo{
							Exists: true,
						},
					},
				},
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:          false,
				WriteBitmap:         true,
				WriteMultiPackIndex: true,
			},
		},
		{
			desc: "no repack needed with multi-pack-index",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					Packfiles: stats.PackfilesInfo{
						Count:             1,
						HasMultiPackIndex: true,
					},
				},
			},
			expectedNeeded: false,
			expectedConfig: RepackObjectsConfig{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repackNeeded, repackCfg := tc.strategy.ShouldRepackObjects(ctx)
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
				alternates        []string
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
					alternates:        []string{"something"},
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
						info: stats.RepositoryInfo{
							IsObjectPool: tc.isPool,
							Packfiles: stats.PackfilesInfo{
								Size:  outerTC.packfileSizeInMB * 1024 * 1024,
								Count: tc.requiredPackfiles - 1,
								Bitmap: stats.BitmapInfo{
									Exists: true,
								},
								HasMultiPackIndex: true,
							},
							Alternates: tc.alternates,
						},
					}

					repackNeeded, _ := strategy.ShouldRepackObjects(ctx)
					require.False(t, repackNeeded)

					// Now we add the last packfile that should bring us across
					// the boundary of having to repack.
					strategy.info.Packfiles.Count++

					repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
					require.True(t, repackNeeded)
					require.Equal(t, RepackObjectsConfig{
						FullRepack:          true,
						WriteBitmap:         len(tc.alternates) == 0,
						WriteMultiPackIndex: true,
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
					info: stats.RepositoryInfo{
						IsObjectPool: tc.isPool,
						LooseObjects: stats.LooseObjectsInfo{
							Count: outerTC.looseObjects,
						},
						Packfiles: stats.PackfilesInfo{
							// We need to pretend that we have a bitmap,
							// otherwise we aways do a full repack.
							Bitmap: stats.BitmapInfo{
								Exists: true,
							},
							HasMultiPackIndex: true,
						},
					},
				}

				repackNeeded, repackCfg := strategy.ShouldRepackObjects(ctx)
				require.Equal(t, outerTC.expectedRepack, repackNeeded)
				require.Equal(t, RepackObjectsConfig{
					FullRepack:          false,
					WriteBitmap:         repackNeeded,
					WriteMultiPackIndex: repackNeeded,
				}, repackCfg)
			})
		}
	}
}

func TestHeuristicalOptimizationStrategy_ShouldPruneObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

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
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 10000,
					},
				},
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "few stale objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 1000,
					},
				},
			},
			expectedShouldPruneObjects: false,
		},
		{
			desc: "too many stale objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 1025,
					},
				},
			},
			expectedShouldPruneObjects: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("normal repository", func(t *testing.T) {
				require.Equal(t, tc.expectedShouldPruneObjects, tc.strategy.ShouldPruneObjects(ctx))
			})

			t.Run("object pool", func(t *testing.T) {
				strategy := tc.strategy
				strategy.info.IsObjectPool = true
				require.False(t, strategy.ShouldPruneObjects(ctx))
			})
		})
	}
}

func TestHeuristicalOptimizationStrategy_ShouldRepackReferences(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	const kiloByte = 1024

	for _, tc := range []struct {
		packedRefsSize uint64
		requiredRefs   uint64
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
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						PackedReferencesSize: tc.packedRefsSize,
						LooseReferencesCount: tc.requiredRefs - 1,
					},
				},
			}

			require.False(t, strategy.ShouldRepackReferences(ctx))

			strategy.info.References.LooseReferencesCount++

			require.True(t, strategy.ShouldRepackReferences(ctx))
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
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
				},
			},
			expectedNeeded: false,
		},
		{
			desc: "repository without bloom filters",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
				},
			},
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository without bloom filters with repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
				},
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with split commit-graph with bitmap without repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						CommitGraphChainLength: 1,
						HasBloomFilters:        true,
					},
				},
			},
			// If we have no generation data then we want to rewrite the commit-graph,
			// but only if the feature flag is enabled.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with split commit-graph and generation data with bitmap without repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						CommitGraphChainLength: 1,
						HasBloomFilters:        true,
						HasGenerationData:      true,
					},
				},
			},
			// We use the information about whether we repacked objects as an indicator
			// whether something has changed in the repository. If it didn't, then we
			// assume no new objects exist and thus we don't rewrite the commit-graph.
			expectedNeeded: false,
		},
		{
			desc: "repository with monolithic commit-graph with bloom filters with repack",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						Count: 9000,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						HasBloomFilters: true,
					},
				},
			},
			// When we have a valid commit-graph, but objects have been repacked, we
			// assume that there are new objects in the repository. So consequentially,
			// we should write the commit-graphs.
			expectedNeeded: true,
			expectedCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		},
		{
			desc: "repository with monolithic commit-graph with bloom filters with pruned objects",
			strategy: HeuristicalOptimizationStrategy{
				info: stats.RepositoryInfo{
					LooseObjects: stats.LooseObjectsInfo{
						StaleCount: 9000,
					},
					References: stats.ReferencesInfo{
						LooseReferencesCount: 1,
					},
					CommitGraph: stats.CommitGraphInfo{
						HasBloomFilters: true,
					},
				},
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
			ctx := testhelper.Context(t)

			needed, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph(ctx)
			require.Equal(t, tc.expectedNeeded, needed)
			require.Equal(t, tc.expectedCfg, writeCommitGraphCfg)
		})
	}
}

func TestEagerOptimizationStrategy(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

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
				info: stats.RepositoryInfo{
					Alternates: []string{"path/to/alternate"},
				},
			},
			expectWriteBitmap:        false,
			expectShouldPruneObjects: true,
		},
		{
			desc: "object pool",
			strategy: EagerOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
				},
			},
			expectWriteBitmap:        true,
			expectShouldPruneObjects: false,
		},
		{
			desc: "object pool with alternate",
			strategy: EagerOptimizationStrategy{
				info: stats.RepositoryInfo{
					IsObjectPool: true,
					Alternates:   []string{"path/to/alternate"},
				},
			},
			expectWriteBitmap:        false,
			expectShouldPruneObjects: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			shouldRepackObjects, repackObjectsCfg := tc.strategy.ShouldRepackObjects(ctx)
			require.True(t, shouldRepackObjects)
			require.Equal(t, RepackObjectsConfig{
				FullRepack:          true,
				WriteBitmap:         tc.expectWriteBitmap,
				WriteMultiPackIndex: true,
			}, repackObjectsCfg)

			shouldWriteCommitGraph, writeCommitGraphCfg := tc.strategy.ShouldWriteCommitGraph(ctx)
			require.True(t, shouldWriteCommitGraph)
			require.Equal(t, WriteCommitGraphConfig{
				ReplaceChain: true,
			}, writeCommitGraphCfg)

			require.Equal(t, tc.expectShouldPruneObjects, tc.strategy.ShouldPruneObjects(ctx))
			require.True(t, tc.strategy.ShouldRepackReferences(ctx))
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

func (m mockOptimizationStrategy) ShouldRepackObjects(context.Context) (bool, RepackObjectsConfig) {
	return m.shouldRepackObjects, m.repackObjectsCfg
}

func (m mockOptimizationStrategy) ShouldPruneObjects(context.Context) bool {
	return m.shouldPruneObjects
}

func (m mockOptimizationStrategy) ShouldRepackReferences(context.Context) bool {
	return m.shouldRepackReferences
}

func (m mockOptimizationStrategy) ShouldWriteCommitGraph(context.Context) (bool, WriteCommitGraphConfig) {
	return m.shouldWriteCommitGraph, m.writeCommitGraphCfg
}
