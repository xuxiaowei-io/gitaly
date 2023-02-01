package housekeeping

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	gitalycfgprom "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
)

// Manager is a housekeeping manager. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type Manager interface {
	// CleanStaleData removes any stale data in the repository.
	CleanStaleData(context.Context, *localrepo.Repo) error
	// OptimizeRepository optimizes the repository's data structures such that it can be more
	// efficiently served.
	OptimizeRepository(context.Context, *localrepo.Repo, ...OptimizeRepositoryOption) error
}

// RepositoryManager is an implementation of the Manager interface.
type RepositoryManager struct {
	txManager transaction.Manager

	tasksTotal             *prometheus.CounterVec
	tasksLatency           *prometheus.HistogramVec
	prunedFilesTotal       *prometheus.CounterVec
	dataStructureExistence *prometheus.CounterVec
	dataStructureCount     *prometheus.HistogramVec
	dataStructureSize      *prometheus.HistogramVec
	optimizeFunc           func(context.Context, *RepositoryManager, *localrepo.Repo, OptimizationStrategy) error
	reposInProgress        sync.Map
}

// NewManager creates a new RepositoryManager.
func NewManager(promCfg gitalycfgprom.Config, txManager transaction.Manager) *RepositoryManager {
	return &RepositoryManager{
		txManager: txManager,

		tasksTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_tasks_total",
				Help: "Total number of housekeeping tasks performed in the repository",
			},
			[]string{"housekeeping_task", "status"},
		),
		tasksLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_tasks_latency",
				Help:    "Latency of the housekeeping tasks performed",
				Buckets: promCfg.GRPCLatencyBuckets,
			},
			[]string{"housekeeping_task"},
		),
		prunedFilesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_pruned_files_total",
				Help: "Total number of files pruned",
			},
			[]string{"filetype"},
		),
		dataStructureExistence: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_data_structure_existence_total",
				Help: "Total number of data structures that exist in the repository",
			},
			[]string{"data_structure", "exists"},
		),
		dataStructureCount: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_data_structure_count",
				Help:    "Total count of the data structures that exist in the repository",
				Buckets: prometheus.ExponentialBucketsRange(1, 10_000_000, 32),
			},
			[]string{"data_structure"},
		),
		dataStructureSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_housekeeping_data_structure_size",
				Help:    "Total size of the data structures that exist in the repository",
				Buckets: prometheus.ExponentialBucketsRange(1, 50_000_000_000, 32),
			},
			[]string{"data_structure"},
		),
		optimizeFunc: optimizeRepository,
	}
}

// Describe is used to describe Prometheus metrics.
func (m *RepositoryManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *RepositoryManager) Collect(metrics chan<- prometheus.Metric) {
	m.tasksTotal.Collect(metrics)
	m.tasksLatency.Collect(metrics)
	m.prunedFilesTotal.Collect(metrics)
	m.dataStructureExistence.Collect(metrics)
	m.dataStructureCount.Collect(metrics)
	m.dataStructureSize.Collect(metrics)
}
