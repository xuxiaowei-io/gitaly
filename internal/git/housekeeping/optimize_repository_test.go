package housekeeping

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestRepackIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("no repacking", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Create a loose object to verify it's not getting repacked.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithMessage("a"))

		didRepack, repackObjectsCfg, err := repackIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldRepackObjects: false,
		})
		require.NoError(t, err)
		require.False(t, didRepack)
		require.Equal(t, RepackObjectsConfig{}, repackObjectsCfg)

		requireObjectsState(t, repo, objectsState{
			looseObjects: 2,
		})
	})

	t.Run("incremental repack", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Create an object and pack it into a packfile.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithMessage("a"))
		gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")
		// And a second object that is loose. The incremental repack should only pack the
		// loose object.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithMessage("b"))

		didRepack, repackObjectsCfg, err := repackIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldRepackObjects: true,
		})
		require.NoError(t, err)
		require.True(t, didRepack)
		require.Equal(t, RepackObjectsConfig{}, repackObjectsCfg)

		requireObjectsState(t, repo, objectsState{
			packfiles: 2,
			hasBitmap: true,
		})
	})

	t.Run("full repack", func(t *testing.T) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Create an object and pack it into a packfile.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("a"), gittest.WithMessage("a"))
		gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")
		// And a second object that is loose. The full repack should repack both the
		// packfiles and loose objects into a single packfile.
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("b"), gittest.WithMessage("b"))

		didRepack, repackObjectsCfg, err := repackIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldRepackObjects: true,
			repackObjectsCfg: RepackObjectsConfig{
				FullRepack: true,
			},
		})
		require.NoError(t, err)
		require.True(t, didRepack)
		require.Equal(t, RepackObjectsConfig{
			FullRepack: true,
		}, repackObjectsCfg)

		requireObjectsState(t, repo, objectsState{
			packfiles: 1,
		})
	})
}

func TestPackRefsIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Write an empty commit such that we can create valid refs.
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

	packedRefsPath := filepath.Join(repoPath, "packed-refs")
	looseRefPath := filepath.Join(repoPath, "refs", "heads", "main")

	// We shouldn't pack refs when the strategy says not to.
	didRepack, err := packRefsIfNeeded(ctx, repo, mockOptimizationStrategy{
		shouldRepackReferences: false,
	})
	require.NoError(t, err)
	require.False(t, didRepack)
	// So there should only be the loose reference now.
	require.NoFileExists(t, packedRefsPath)
	require.FileExists(t, looseRefPath)

	// On the other hand, we should repack if the strategy tells us to.
	didRepack, err = packRefsIfNeeded(ctx, repo, mockOptimizationStrategy{
		shouldRepackReferences: true,
	})
	require.NoError(t, err)
	require.True(t, didRepack)
	// There should only be the packed-refs file now.
	require.FileExists(t, packedRefsPath)
	require.NoFileExists(t, looseRefPath)
}

func TestOptimizeRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	type metric struct {
		name, status string
		count        int
	}

	for _, tc := range []struct {
		desc                   string
		setup                  func(t *testing.T, relativePath string) *gitalypb.Repository
		expectedErr            error
		expectedMetrics        []metric
		expectedMetricsForPool []metric
	}{
		{
			desc: "empty repository does nothing",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				return repo
			},
			expectedMetrics: []metric{
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "repository without bitmap repacks objects",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_commit_graph_full", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "repository without commit-graph writes commit-graph",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index")
				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_full", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "repository with commit-graph without generation data writes commit-graph",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=1", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")
				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_full", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "repository without multi-pack-index performs incremental repack",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "-b")
				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_full", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "repository with multiple packfiles packs only for object pool",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				// Create two packfiles by creating two objects and then packing
				// twice. Note that the second git-repack(1) is incremental so that
				// we don't remove the first packfile.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("first"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("second"), gittest.WithMessage("second"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack")

				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
			expectedMetricsForPool: []metric{
				{name: "packed_objects_full", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "well-packed repository does not optimize",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")
				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "well-packed repository with multi-pack-index does not optimize",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index", "--write-midx")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")
				return repo
			},
			expectedMetrics: []metric{
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "recent loose objects don't get pruned",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				// The repack won't repack the following objects because they're
				// broken, and thus we'll retry to prune them afterwards.
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "17"), perm.SharedDir))

				// We set the object's mtime to be almost two weeks ago. Given that
				// our timeout is at exactly two weeks this shouldn't caused them to
				// get pruned.
				almostTwoWeeksAgo := time.Now().Add(stats.StaleObjectsGracePeriod).Add(time.Minute)

				for i := 0; i < looseObjectLimit+1; i++ {
					blobPath := filepath.Join(repoPath, "objects", "17", fmt.Sprintf("%d", i))
					require.NoError(t, os.WriteFile(blobPath, nil, perm.SharedFile))
					require.NoError(t, os.Chtimes(blobPath, almostTwoWeeksAgo, almostTwoWeeksAgo))
				}

				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "old loose objects get pruned",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-d", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				// The repack won't repack the following objects because they're
				// broken, and thus we'll retry to prune them afterwards.
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "17"), perm.SharedDir))

				moreThanTwoWeeksAgo := time.Now().Add(stats.StaleObjectsGracePeriod).Add(-time.Minute)

				for i := 0; i < looseObjectLimit+1; i++ {
					blobPath := filepath.Join(repoPath, "objects", "17", fmt.Sprintf("%d", i))
					require.NoError(t, os.WriteFile(blobPath, nil, perm.SharedFile))
					require.NoError(t, os.Chtimes(blobPath, moreThanTwoWeeksAgo, moreThanTwoWeeksAgo))
				}

				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "pruned_objects", status: "success", count: 1},
				{name: "written_commit_graph_full", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
			// Object pools never prune objects.
			expectedMetricsForPool: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
		{
			desc: "loose refs get packed",
			setup: func(t *testing.T, relativePath string) *gitalypb.Repository {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
					RelativePath:           relativePath,
				})

				for i := 0; i < 16; i++ {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(fmt.Sprintf("branch-%d", i)))
				}

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-c", "commitGraph.generationVersion=2", "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				return repo
			},
			expectedMetrics: []metric{
				{name: "packed_objects_incremental", status: "success", count: 1},
				{name: "packed_refs", status: "success", count: 1},
				{name: "written_commit_graph_incremental", status: "success", count: 1},
				{name: "written_bitmap", status: "success", count: 1},
				{name: "written_multi_pack_index", status: "success", count: 1},
				{name: "total", status: "success", count: 1},
			},
		},
	} {
		tc := tc

		testRepoAndPool(t, tc.desc, func(t *testing.T, relativePath string) {
			t.Parallel()

			repoProto := tc.setup(t, relativePath)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			manager := NewManager(cfg.Prometheus, txManager)

			err := manager.OptimizeRepository(ctx, repo)
			require.Equal(t, tc.expectedErr, err)

			expectedMetrics := tc.expectedMetrics
			if stats.IsPoolRepository(repoProto) && tc.expectedMetricsForPool != nil {
				expectedMetrics = tc.expectedMetricsForPool
			}

			var buf bytes.Buffer
			_, err = buf.WriteString("# HELP gitaly_housekeeping_tasks_total Total number of housekeeping tasks performed in the repository\n")
			require.NoError(t, err)
			_, err = buf.WriteString("# TYPE gitaly_housekeeping_tasks_total counter\n")
			require.NoError(t, err)

			for _, metric := range expectedMetrics {
				_, err := buf.WriteString(fmt.Sprintf(
					"gitaly_housekeeping_tasks_total{housekeeping_task=%q, status=%q} %d\n",
					metric.name, metric.status, metric.count,
				))
				require.NoError(t, err)
			}

			require.NoError(t, testutil.CollectAndCompare(
				manager.tasksTotal, &buf, "gitaly_housekeeping_tasks_total",
			))
		})
	}
}

func TestOptimizeRepository_ConcurrencyLimit(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("subsequent calls get skipped", func(t *testing.T) {
		reqReceivedCh, ch := make(chan struct{}), make(chan struct{})

		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		manager := NewManager(gitalycfgprom.Config{}, nil)
		manager.optimizeFunc = func(context.Context, *RepositoryManager, *localrepo.Repo, OptimizationStrategy) error {
			reqReceivedCh <- struct{}{}
			ch <- struct{}{}

			return nil
		}

		go func() {
			require.NoError(t, manager.OptimizeRepository(ctx, repo))
		}()

		<-reqReceivedCh
		// When repository optimizations are performed for a specific repository already,
		// then any subsequent calls to the same repository should just return immediately
		// without doing any optimizations at all.
		require.NoError(t, manager.OptimizeRepository(ctx, repo))

		<-ch
	})

	t.Run("multiple repositories concurrently", func(t *testing.T) {
		reqReceivedCh, ch := make(chan struct{}), make(chan struct{})

		repoProtoFirst, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repoFirst := localrepo.NewTestRepo(t, cfg, repoProtoFirst)
		repoProtoSecond, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repoSecond := localrepo.NewTestRepo(t, cfg, repoProtoSecond)

		reposOptimized := make(map[string]struct{})

		manager := NewManager(gitalycfgprom.Config{}, nil)
		manager.optimizeFunc = func(_ context.Context, _ *RepositoryManager, repo *localrepo.Repo, _ OptimizationStrategy) error {
			reposOptimized[repo.GetRelativePath()] = struct{}{}

			if repo.GitRepo.GetRelativePath() == repoFirst.GetRelativePath() {
				reqReceivedCh <- struct{}{}
				ch <- struct{}{}
			}

			return nil
		}

		// We block in the first call so that we can assert that a second call
		// to a different repository performs the optimization regardless without blocking.
		go func() {
			require.NoError(t, manager.OptimizeRepository(ctx, repoFirst))
		}()

		<-reqReceivedCh

		// Because this optimizes a different repository this call shouldn't block.
		require.NoError(t, manager.OptimizeRepository(ctx, repoSecond))

		<-ch

		assert.Contains(t, reposOptimized, repoFirst.GetRelativePath())
		assert.Contains(t, reposOptimized, repoSecond.GetRelativePath())
	})

	t.Run("serialized optimizations", func(t *testing.T) {
		reqReceivedCh, ch := make(chan struct{}), make(chan struct{})
		repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)
		var optimizations int

		manager := NewManager(gitalycfgprom.Config{}, nil)
		manager.optimizeFunc = func(context.Context, *RepositoryManager, *localrepo.Repo, OptimizationStrategy) error {
			optimizations++

			if optimizations == 1 {
				reqReceivedCh <- struct{}{}
				ch <- struct{}{}
			}

			return nil
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, manager.OptimizeRepository(ctx, repo))
		}()

		<-reqReceivedCh

		// Because we already have a concurrent call which optimizes the repository we expect
		// that all subsequent calls which try to optimize the same repository return immediately.
		// Furthermore, we expect to see only a single call to the optimizing function because we
		// don't want to optimize the same repository concurrently.
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		assert.Equal(t, 1, optimizations)

		<-ch
		wg.Wait()

		// When performing optimizations sequentially though the repository
		// should be unlocked after every call, and consequentially we should
		// also see multiple calls to the optimizing function.
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		require.NoError(t, manager.OptimizeRepository(ctx, repo))
		assert.Equal(t, 4, optimizations)
	})
}

func TestPruneIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	objectPath := func(oid git.ObjectID) string {
		return filepath.Join(repoPath, "objects", oid.String()[0:2], oid.String()[2:])
	}

	// Write two blobs, one recent blob and one blob that is older than two weeks and that would
	// thus get pruned.
	recentBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("recent"))
	staleBlobID := gittest.WriteBlob(t, cfg, repoPath, []byte("stale"))
	twoWeeksAgo := time.Now().Add(-1 * 2 * 7 * 24 * time.Hour)
	require.NoError(t, os.Chtimes(objectPath(staleBlobID), twoWeeksAgo, twoWeeksAgo))

	// We shouldn't prune when the strategy determines there aren't enough old objects.
	didPrune, err := pruneIfNeeded(ctx, repo, mockOptimizationStrategy{
		shouldPruneObjects: false,
	})
	require.NoError(t, err)
	require.False(t, didPrune)

	// Consequentially, the objects shouldn't have been pruned.
	require.FileExists(t, objectPath(recentBlobID))
	require.FileExists(t, objectPath(staleBlobID))

	// But we naturally should prune if told so.
	didPrune, err = pruneIfNeeded(ctx, repo, mockOptimizationStrategy{
		shouldPruneObjects: true,
	})
	require.NoError(t, err)
	require.True(t, didPrune)

	// But we should only prune the stale blob, never the recent one.
	require.FileExists(t, objectPath(recentBlobID))
	require.NoFileExists(t, objectPath(staleBlobID))
}

func TestWriteCommitGraphIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	t.Run("strategy does not update commit-graph", func(t *testing.T) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

		written, cfg, err := writeCommitGraphIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldWriteCommitGraph: false,
		})
		require.NoError(t, err)
		require.False(t, written)
		require.Equal(t, WriteCommitGraphConfig{}, cfg)

		require.NoFileExists(t, filepath.Join(repoPath, "objects", "info", "commit-graph"))
		require.NoDirExists(t, filepath.Join(repoPath, "objects", "info", "commit-graphs"))
	})

	t.Run("strategy does update commit-graph", func(t *testing.T) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

		written, cfg, err := writeCommitGraphIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldWriteCommitGraph: true,
		})
		require.NoError(t, err)
		require.True(t, written)
		require.Equal(t, WriteCommitGraphConfig{}, cfg)

		require.NoFileExists(t, filepath.Join(repoPath, "objects", "info", "commit-graph"))
		require.DirExists(t, filepath.Join(repoPath, "objects", "info", "commit-graphs"))
	})

	t.Run("commit-graph with pruned objects", func(t *testing.T) {
		t.Parallel()

		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

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

		// Write the commit-graph incrementally.
		didWrite, writeCommitGraphCfg, err := writeCommitGraphIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldWriteCommitGraph: true,
		})
		require.NoError(t, err)
		require.True(t, didWrite)
		require.Equal(t, WriteCommitGraphConfig{}, writeCommitGraphCfg)

		// We should still observe the failure failure.
		stderr.Reset()
		verifyCmd = gittest.NewCommand(t, cfg, "-C", repoPath, "commit-graph", "verify")
		verifyCmd.Stderr = &stderr
		require.EqualError(t, verifyCmd.Run(), "exit status 1")
		require.Equal(t, stderr.String(), fmt.Sprintf("error: Could not read %[1]s\nfailed to parse commit %[1]s from object database for commit-graph\n", unreachableCommitID))

		// Write the commit-graph a second time, but this time we ask to rewrite the
		// commit-graph completely.
		didWrite, writeCommitGraphCfg, err = writeCommitGraphIfNeeded(ctx, repo, mockOptimizationStrategy{
			shouldWriteCommitGraph: true,
			writeCommitGraphCfg: WriteCommitGraphConfig{
				ReplaceChain: true,
			},
		})
		require.NoError(t, err)
		require.True(t, didWrite)
		require.Equal(t, WriteCommitGraphConfig{
			ReplaceChain: true,
		}, writeCommitGraphCfg)

		// The commit-graph should now have been fixed.
		gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "verify")
	})
}
