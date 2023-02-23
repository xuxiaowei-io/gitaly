package housekeeping

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/tracing"
)

// OptimizeRepositoryConfig is the configuration used by OptimizeRepository that is computed by
// applying all the OptimizeRepositoryOption modifiers.
type OptimizeRepositoryConfig struct {
	StrategyConstructor OptimizationStrategyConstructor
}

// OptimizeRepositoryOption is an option that can be passed to OptimizeRepository.
type OptimizeRepositoryOption func(cfg *OptimizeRepositoryConfig)

// OptimizationStrategyConstructor is a constructor for an OptimizationStrategy that is being
// informed by the passed-in RepositoryInfo.
type OptimizationStrategyConstructor func(stats.RepositoryInfo) OptimizationStrategy

// WithOptimizationStrategyConstructor changes the constructor for the optimization strategy.that is
// used to determine which parts of the repository will be optimized. By default the
// HeuristicalOptimizationStrategy is used.
func WithOptimizationStrategyConstructor(strategyConstructor OptimizationStrategyConstructor) OptimizeRepositoryOption {
	return func(cfg *OptimizeRepositoryConfig) {
		cfg.StrategyConstructor = strategyConstructor
	}
}

// OptimizeRepository performs optimizations on the repository. Whether optimizations are performed
// or not depends on a set of heuristics.
func (m *RepositoryManager) OptimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	opts ...OptimizeRepositoryOption,
) error {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "housekeeping.OptimizeRepository", nil)
	defer span.Finish()

	path, err := repo.Path()
	if err != nil {
		return err
	}

	if _, ok := m.reposInProgress.LoadOrStore(path, struct{}{}); ok {
		return nil
	}

	defer func() {
		m.reposInProgress.Delete(path)
	}()

	var cfg OptimizeRepositoryConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	repositoryInfo, err := stats.RepositoryInfoForRepository(repo)
	if err != nil {
		return fmt.Errorf("deriving repository info: %w", err)
	}
	m.reportRepositoryInfo(ctx, repositoryInfo)

	var strategy OptimizationStrategy
	if cfg.StrategyConstructor == nil {
		strategy = NewHeuristicalOptimizationStrategy(repositoryInfo)
	} else {
		strategy = cfg.StrategyConstructor(repositoryInfo)
	}

	return m.optimizeFunc(ctx, m, repo, strategy)
}

func (m *RepositoryManager) reportRepositoryInfo(ctx context.Context, info stats.RepositoryInfo) {
	info.Log(ctx)

	m.reportDataStructureExistence("commit_graph", info.CommitGraph.Exists)
	m.reportDataStructureExistence("commit_graph_bloom_filters", info.CommitGraph.HasBloomFilters)
	m.reportDataStructureExistence("commit_graph_generation_data", info.CommitGraph.HasGenerationData)
	m.reportDataStructureExistence("commit_graph_generation_data_overflow", info.CommitGraph.HasGenerationDataOverflow)
	m.reportDataStructureExistence("bitmap", info.Packfiles.Bitmap.Exists)
	m.reportDataStructureExistence("bitmap_hash_cache", info.Packfiles.Bitmap.HasHashCache)
	m.reportDataStructureExistence("bitmap_lookup_table", info.Packfiles.Bitmap.HasLookupTable)
	m.reportDataStructureExistence("multi_pack_index", info.Packfiles.HasMultiPackIndex)
	m.reportDataStructureExistence("multi_pack_index_bitmap", info.Packfiles.MultiPackIndexBitmap.Exists)
	m.reportDataStructureExistence("multi_pack_index_bitmap_hash_cache", info.Packfiles.MultiPackIndexBitmap.HasHashCache)
	m.reportDataStructureExistence("multi_pack_index_bitmap_lookup_table", info.Packfiles.MultiPackIndexBitmap.HasLookupTable)

	m.reportDataStructureCount("loose_objects_recent", info.LooseObjects.Count-info.LooseObjects.StaleCount)
	m.reportDataStructureCount("loose_objects_stale", info.LooseObjects.StaleCount)
	m.reportDataStructureCount("commit_graph_chain", info.CommitGraph.CommitGraphChainLength)
	m.reportDataStructureCount("packfiles", info.Packfiles.Count)
	m.reportDataStructureCount("packfiles_cruft", info.Packfiles.CruftCount)
	m.reportDataStructureCount("packfiles_keep", info.Packfiles.KeepCount)
	m.reportDataStructureCount("packfiles_reverse_indices", info.Packfiles.ReverseIndexCount)
	m.reportDataStructureCount("loose_references", info.References.LooseReferencesCount)

	m.reportDataStructureSize("loose_objects_recent", info.LooseObjects.Size-info.LooseObjects.StaleSize)
	m.reportDataStructureSize("loose_objects_stale", info.LooseObjects.StaleSize)
	m.reportDataStructureSize("packfiles", info.Packfiles.Size)
	m.reportDataStructureSize("packfiles_cruft", info.Packfiles.CruftSize)
	m.reportDataStructureSize("packfiles_keep", info.Packfiles.KeepSize)
	m.reportDataStructureSize("packed_references", info.References.PackedReferencesSize)
}

func (m *RepositoryManager) reportDataStructureExistence(dataStructure string, exists bool) {
	m.dataStructureExistence.WithLabelValues(dataStructure, strconv.FormatBool(exists)).Inc()
	// We also report the inverse metric so that it will be visible to clients.
	m.dataStructureExistence.WithLabelValues(dataStructure, strconv.FormatBool(!exists)).Add(0)
}

func (m *RepositoryManager) reportDataStructureCount(dataStructure string, count uint64) {
	m.dataStructureCount.WithLabelValues(dataStructure).Observe(float64(count))
}

func (m *RepositoryManager) reportDataStructureSize(dataStructure string, size uint64) {
	m.dataStructureSize.WithLabelValues(dataStructure).Observe(float64(size))
}

func optimizeRepository(
	ctx context.Context,
	m *RepositoryManager,
	repo *localrepo.Repo,
	strategy OptimizationStrategy,
) error {
	totalTimer := prometheus.NewTimer(m.tasksLatency.WithLabelValues("total"))
	totalStatus := "failure"

	optimizations := make(map[string]string)
	defer func() {
		totalTimer.ObserveDuration()
		ctxlogrus.Extract(ctx).WithField("optimizations", optimizations).Info("optimized repository")

		for task, status := range optimizations {
			m.tasksTotal.WithLabelValues(task, status).Inc()
		}

		m.tasksTotal.WithLabelValues("total", totalStatus).Add(1)
	}()

	timer := prometheus.NewTimer(m.tasksLatency.WithLabelValues("clean-stale-data"))
	if err := m.CleanStaleData(ctx, repo); err != nil {
		return fmt.Errorf("could not execute houskeeping: %w", err)
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("clean-worktrees"))
	if err := CleanupWorktrees(ctx, repo); err != nil {
		return fmt.Errorf("could not clean up worktrees: %w", err)
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("repack"))
	didRepack, repackCfg, err := repackIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_objects_full"] = "failure"
		optimizations["packed_objects_incremental"] = "failure"
		optimizations["written_bitmap"] = "failure"
		optimizations["written_multi_pack_index"] = "failure"
		return fmt.Errorf("could not repack: %w", err)
	}
	if didRepack {
		if repackCfg.FullRepack {
			optimizations["packed_objects_full"] = "success"
		} else {
			optimizations["packed_objects_incremental"] = "success"
		}
		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = "success"
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = "success"
		}
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("prune"))
	didPrune, err := pruneIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["pruned_objects"] = "failure"
		return fmt.Errorf("could not prune: %w", err)
	} else if didPrune {
		optimizations["pruned_objects"] = "success"
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("pack-refs"))
	didPackRefs, err := packRefsIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_refs"] = "failure"
		return fmt.Errorf("could not pack refs: %w", err)
	} else if didPackRefs {
		optimizations["packed_refs"] = "success"
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("commit-graph"))
	if didWriteCommitGraph, writeCommitGraphCfg, err := writeCommitGraphIfNeeded(ctx, repo, strategy); err != nil {
		optimizations["written_commit_graph_full"] = "failure"
		optimizations["written_commit_graph_incremental"] = "failure"
		return fmt.Errorf("could not write commit-graph: %w", err)
	} else if didWriteCommitGraph {
		if writeCommitGraphCfg.ReplaceChain {
			optimizations["written_commit_graph_full"] = "success"
		} else {
			optimizations["written_commit_graph_incremental"] = "success"
		}
	}
	timer.ObserveDuration()

	totalStatus = "success"

	return nil
}

// repackIfNeeded repacks the repository according to the strategy.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy OptimizationStrategy) (bool, RepackObjectsConfig, error) {
	repackNeeded, cfg := strategy.ShouldRepackObjects(ctx)
	if !repackNeeded {
		return false, RepackObjectsConfig{}, nil
	}

	if err := RepackObjects(ctx, repo, cfg); err != nil {
		return false, RepackObjectsConfig{}, err
	}

	return true, cfg, nil
}

// writeCommitGraphIfNeeded writes the commit-graph if required.
func writeCommitGraphIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy OptimizationStrategy) (bool, WriteCommitGraphConfig, error) {
	needed, cfg := strategy.ShouldWriteCommitGraph(ctx)
	if !needed {
		return false, WriteCommitGraphConfig{}, nil
	}

	if err := WriteCommitGraph(ctx, repo, cfg); err != nil {
		return true, cfg, fmt.Errorf("writing commit-graph: %w", err)
	}

	return true, cfg, nil
}

// pruneIfNeeded removes objects from the repository which are either unreachable or which are
// already part of a packfile. We use a grace period of two weeks.
func pruneIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy OptimizationStrategy) (bool, error) {
	if !strategy.ShouldPruneObjects(ctx) {
		return false, nil
	}

	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "prune",
		Flags: []git.Option{
			// By default, this prunes all unreachable objects regardless of when they
			// have last been accessed. This opens us up for races when there are
			// concurrent commands which are just at the point of writing objects into
			// the repository, but which haven't yet updated any references to make them
			// reachable. We thus use the same two-week grace period as git-gc(1) does.
			git.ValueFlag{Name: "--expire", Value: "two.weeks.ago"},
		},
	}); err != nil {
		return false, fmt.Errorf("pruning objects: %w", err)
	}

	return true, nil
}

func packRefsIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy OptimizationStrategy) (bool, error) {
	if !strategy.ShouldRepackReferences(ctx) {
		return false, nil
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.Command{
		Name: "pack-refs",
		Flags: []git.Option{
			git.Flag{Name: "--all"},
		},
	}, git.WithStderr(&stderr)); err != nil {
		return false, fmt.Errorf("packing refs: %w, stderr: %q", err, stderr.String())
	}

	return true, nil
}
