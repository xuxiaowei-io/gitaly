package counter

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/walk"
)

// RepositoryCounter provides metrics with a count of repositories present
// on a Gitaly node.
type RepositoryCounter struct {
	reposTotal     *prometheus.GaugeVec
	suppressMetric atomic.Bool
}

// NewRepositoryCounter constructs a RepositoryCounter object.
func NewRepositoryCounter() *RepositoryCounter {
	c := &RepositoryCounter{
		reposTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_total_repositories_count",
				Help: "Gauge of number of repositories by storage and path",
			},
			[]string{"storage", "prefix"},
		),
	}
	c.suppressMetric.Store(true)

	return c
}

// Describe is used to describe Prometheus metrics.
func (c *RepositoryCounter) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *RepositoryCounter) Collect(metrics chan<- prometheus.Metric) {
	if c.suppressMetric.Load() {
		return
	}

	c.reposTotal.Collect(metrics)
}

// StartCountingRepositories counts the number of repositories on disk in a goroutine.
func (c *RepositoryCounter) StartCountingRepositories(
	ctx context.Context,
	locator storage.Locator,
	storages []config.Storage,
	logger log.FieldLogger,
) {
	dontpanic.Go(func() {
		c.countRepositories(ctx, locator, storages, logger)
	})
}

func (c *RepositoryCounter) countRepositories(
	ctx context.Context,
	locator storage.Locator,
	storages []config.Storage,
	logger log.FieldLogger,
) {
	defer func() {
		c.suppressMetric.Store(false)
	}()

	logger.Info("counting repositories")
	totalStart := time.Now()

	for _, stor := range storages {
		logger.Infof("starting to count repositories in storage %q", stor.Name)
		storageStart := time.Now()

		paths := make(map[string]float64)
		incrementPrefix := func(relPath string, gitDirInfo fs.FileInfo) error {
			prefix, err := getPrefix(relPath)
			if err != nil {
				// Encountering a malformed path should not block us from continuing
				// to count.
				logger.WithError(err).Warnf("counting repositories: walking storage %q", stor)
				return nil
			}

			paths[prefix]++
			return nil
		}
		ctx := context.Background()

		if err := walk.FindRepositories(ctx, locator, stor.Name, incrementPrefix); err != nil {
			log.WithError(err).Errorf("failed to count repositories in storage %q", stor.Name)
		}

		for prefix, ct := range paths {
			c.reposTotal.WithLabelValues(stor.Name, prefix).Add(ct)
		}

		logger.Infof("completed counting repositories in storage %q after %s", stor.Name, time.Since(storageStart))
	}

	logger.Infof("completed counting all repositories after %s", time.Since(totalStart))
}

// Increment increases the repository count by one.
func (c *RepositoryCounter) Increment(repo storage.Repository) {
	c.add(repo, 1)
}

// Decrement decreases the repository count by one.
func (c *RepositoryCounter) Decrement(repo storage.Repository) {
	c.add(repo, -1)
}

// DeleteStorage removes metrics associated with a storage.
func (c *RepositoryCounter) DeleteStorage(storage string) {
	c.reposTotal.DeletePartialMatch(prometheus.Labels{"storage": storage})
}

func (c *RepositoryCounter) add(repo storage.Repository, ct float64) {
	prefix, err := getPrefix(repo.GetRelativePath())
	if err != nil {
		return
	}

	c.reposTotal.WithLabelValues(repo.GetStorageName(), prefix).Add(ct)
}

func getPrefix(path string) (string, error) {
	// Non-Praefect paths have the prefix at the start '@pools/aa/bb/aabb...'.
	prefixIdx := 0

	// '@cluster' paths have the prefix as the second directory '@cluster/pools/aa/bb/1234'.
	if strings.HasPrefix(path, storage.PraefectRootPathPrefix) {
		prefixIdx = 1
	}

	// Split into enough substrings to ensure the prefix is separated.
	splits := strings.SplitN(path, string(os.PathSeparator), prefixIdx+2)
	if len(splits) < prefixIdx+2 {
		return "", fmt.Errorf("malformed repository path %q", path)
	}

	return splits[prefixIdx], nil
}
