package housekeeping

import (
	"bytes"
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
)

// OptimizeRepositoryConfig is the configuration used by OptimizeRepository that is computed by
// applying all the OptimizeRepositoryOption modifiers.
type OptimizeRepositoryConfig struct {
	Strategy OptimizationStrategy
}

// OptimizeRepositoryOption is an option that can be passed to OptimizeRepository.
type OptimizeRepositoryOption func(cfg *OptimizeRepositoryConfig)

// WithOptimizationStrategy changes the strategy used to determine which parts of the repository
// will be optimized. By default the HeuristicalOptimizationStrategy is used.
func WithOptimizationStrategy(strategy OptimizationStrategy) OptimizeRepositoryOption {
	return func(cfg *OptimizeRepositoryConfig) {
		cfg.Strategy = strategy
	}
}

// OptimizeRepository performs optimizations on the repository. Whether optimizations are performed
// or not depends on a set of heuristics.
func (m *RepositoryManager) OptimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	opts ...OptimizeRepositoryOption,
) error {
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

	if cfg.Strategy == nil {
		repositoryInfo, err := stats.RepositoryInfoForRepository(repo)
		if err != nil {
			return fmt.Errorf("deriving repository info: %w", err)
		}
		repositoryInfo.Log(ctx)

		cfg.Strategy = NewHeuristicalOptimizationStrategy(repositoryInfo)
	}

	return m.optimizeFunc(ctx, m, repo, cfg)
}

func optimizeRepository(
	ctx context.Context,
	m *RepositoryManager,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
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
	didRepack, repackCfg, err := repackIfNeeded(ctx, repo, cfg.Strategy)
	if err != nil {
		optimizations["packed_objects_full"] = "failure"
		optimizations["packed_objects_incremental"] = "failure"
		optimizations["written_bitmap"] = "failure"
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
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("prune"))
	didPrune, err := pruneIfNeeded(ctx, repo, cfg.Strategy)
	if err != nil {
		optimizations["pruned_objects"] = "failure"
		return fmt.Errorf("could not prune: %w", err)
	} else if didPrune {
		optimizations["pruned_objects"] = "success"
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("pack-refs"))
	didPackRefs, err := packRefsIfNeeded(ctx, repo, cfg.Strategy)
	if err != nil {
		optimizations["packed_refs"] = "failure"
		return fmt.Errorf("could not pack refs: %w", err)
	} else if didPackRefs {
		optimizations["packed_refs"] = "success"
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("commit-graph"))
	if didWriteCommitGraph, writeCommitGraphCfg, err := writeCommitGraphIfNeeded(ctx, repo, cfg.Strategy); err != nil {
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
