package counter

import (
	"fmt"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// RepositoryCounter provides metrics with a count of repositories present
// on a Gitaly node.
type RepositoryCounter struct {
	reposTotal *prometheus.GaugeVec
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

	return c
}

// Describe is used to describe Prometheus metrics.
func (c *RepositoryCounter) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *RepositoryCounter) Collect(metrics chan<- prometheus.Metric) {
	c.reposTotal.Collect(metrics)
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
