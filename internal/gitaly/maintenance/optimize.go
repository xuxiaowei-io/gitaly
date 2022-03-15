package maintenance

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

var repoOptimizationHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "gitaly_daily_maintenance_repo_optimization_seconds",
		Help:    "How many seconds each repo takes to successfully optimize during daily maintenance",
		Buckets: []float64{0.01, 0.1, 1.0, 10.0, 100},
	},
)

func init() {
	prometheus.MustRegister(repoOptimizationHistogram)
}

func shuffledStoragesCopy(randSrc *rand.Rand, storages []config.Storage) []config.Storage {
	shuffled := make([]config.Storage, len(storages))
	copy(shuffled, storages)
	randSrc.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return shuffled
}

// Optimizer knows how to optimize a repository
type Optimizer interface {
	OptimizeRepository(context.Context, repository.GitRepo) error
}

// OptimizerFunc is an adapter to allow the use of an ordinary function as an Optimizer
type OptimizerFunc func(context.Context, repository.GitRepo) error

// OptimizeRepository calls o(ctx, repo)
func (o OptimizerFunc) OptimizeRepository(ctx context.Context, repo repository.GitRepo) error {
	return o(ctx, repo)
}

// DailyOptimizationWorker creates a worker that runs repository maintenance daily
func DailyOptimizationWorker(cfg config.Cfg, optimizer Optimizer) server.WorkerFunc {
	return func(ctx context.Context, l logrus.FieldLogger) error {
		return NewDailyWorker().StartDaily(
			ctx,
			l,
			cfg.DailyMaintenance,
			OptimizeReposRandomly(
				cfg.Storages,
				optimizer,
				helper.NewTimerTicker(1*time.Second),
				rand.New(rand.NewSource(time.Now().UnixNano())),
			),
		)
	}
}

func optimizeRepoAtPath(ctx context.Context, l logrus.FieldLogger, s config.Storage, absPath string, o Optimizer) error {
	relPath, err := filepath.Rel(s.Path, absPath)
	if err != nil {
		return err
	}

	repo := &gitalypb.Repository{
		StorageName:  s.Name,
		RelativePath: relPath,
	}

	start := time.Now()
	e := l.WithFields(map[string]interface{}{
		"relative_path": relPath,
		"storage":       s.Name,
		"source":        "daily maintenance",
		"start_time":    start.UTC(),
	})

	ctx, cancel := context.WithTimeout(ctxlogrus.ToContext(ctx, e), 5*time.Minute)
	defer cancel()

	err = o.OptimizeRepository(ctx, repo)
	e = e.WithField("time_ms", time.Since(start).Milliseconds())

	if err != nil {
		e.WithError(err).Errorf("maintenance: repo optimization failure")
		return err
	}

	e.Info("maintenance: repo optimization succeeded")
	repoOptimizationHistogram.Observe(time.Since(start).Seconds())

	return nil
}

func walkReposShuffled(
	ctx context.Context,
	walker *randomWalker,
	l logrus.FieldLogger,
	s config.Storage,
	o Optimizer,
	ticker helper.Ticker,
) error {
	for {
		fi, path, err := walker.next()
		switch {
		case errors.Is(err, errIterOver):
			return nil
		case os.IsNotExist(err):
			continue // race condition: someone deleted it
		case err != nil:
			return err
		}

		if !fi.IsDir() || !storage.IsGitDirectory(path) {
			continue
		}
		walker.skipDir()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
		}

		// Reset the ticker before doing the optimization such that we essentially limit
		// ourselves to doing optimizations once per tick, not once per tick plus the time
		// it takes to do the optimization. It's best effort given that traversing the
		// directory hierarchy takes some time, too, but it should be good enough for now.
		ticker.Reset()

		if err := optimizeRepoAtPath(ctx, l, s, path, o); err != nil {
			return err
		}
	}
}

// OptimizeReposRandomly returns a function to walk through each storage and attempts to optimize
// any repos encountered. The ticker is used to rate-limit optimizations.
//
// Only storage paths that map to an enabled storage name will be walked. Any storage paths shared
// by multiple storages will only be walked once.
//
// Any errors during the optimization will be logged. Any other errors will be returned and cause
// the walk to end prematurely.
func OptimizeReposRandomly(storages []config.Storage, optimizer Optimizer, ticker helper.Ticker, rand *rand.Rand) StoragesJob {
	return func(ctx context.Context, l logrus.FieldLogger, enabledStorageNames []string) error {
		enabledNames := map[string]struct{}{}
		for _, sName := range enabledStorageNames {
			enabledNames[sName] = struct{}{}
		}

		visitedPaths := map[string]bool{}

		ticker.Reset()
		defer ticker.Stop()

		for _, storage := range shuffledStoragesCopy(rand, storages) {
			if _, ok := enabledNames[storage.Name]; !ok {
				continue // storage not enabled
			}
			if visitedPaths[storage.Path] {
				continue // already visited
			}
			visitedPaths[storage.Path] = true

			l.WithField("storage_path", storage.Path).
				Info("maintenance: optimizing repos in storage")

			walker := newRandomWalker(storage.Path, rand)

			if err := walkReposShuffled(ctx, walker, l, storage, optimizer, ticker); err != nil {
				l.WithError(err).
					WithField("storage_path", storage.Path).
					Errorf("maintenance: unable to completely walk storage")
			}
		}
		return nil
	}
}
