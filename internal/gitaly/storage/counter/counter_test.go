package counter

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type metric struct {
	storage, prefix string
	count           int
}

func TestCounter(t *testing.T) {
	t.Parallel()

	const storageName = "default"
	repo := &gitalypb.Repository{
		StorageName:  storageName,
		RelativePath: gittest.NewRepositoryName(t),
	}

	for _, tc := range []struct {
		name            string
		setup           func(t *testing.T, c *RepositoryCounter)
		expectedMetrics []metric
	}{
		{
			name: "increment",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Increment(repo)
			},
			expectedMetrics: []metric{{storage: storageName, prefix: "@hashed", count: 1}},
		},
		{
			name: "decrement",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Decrement(repo)
			},
			expectedMetrics: []metric{{storage: storageName, prefix: "@hashed", count: -1}},
		},
		{
			name: "object pool",
			setup: func(t *testing.T, c *RepositoryCounter) {
				poolRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: gittest.NewObjectPoolName(t),
				}
				c.Increment(poolRepo)
			},
			expectedMetrics: []metric{{storage: storageName, prefix: "@pools", count: 1}},
		},
		{
			name: "praefect",
			setup: func(t *testing.T, c *RepositoryCounter) {
				praefectRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: storage.DeriveReplicaPath(1),
				}
				praefectPool := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: storage.DerivePoolPath(2),
				}
				c.Increment(praefectRepo)
				c.Increment(praefectPool)
			},
			expectedMetrics: []metric{
				{storage: storageName, prefix: "repositories", count: 1},
				{storage: storageName, prefix: "pools", count: 1},
			},
		},
		{
			name: "delete storage",
			setup: func(t *testing.T, c *RepositoryCounter) {
				otherRepo := &gitalypb.Repository{
					StorageName:  "other",
					RelativePath: gittest.NewRepositoryName(t),
				}
				c.Increment(repo)
				c.Increment(otherRepo)

				c.DeleteStorage(storageName)
			},
			expectedMetrics: []metric{{storage: "other", prefix: "@hashed", count: 1}},
		},
		{
			name: "invalid path ignored",
			setup: func(t *testing.T, c *RepositoryCounter) {
				invalidRepo := &gitalypb.Repository{
					StorageName:  storageName,
					RelativePath: "bad",
				}
				c.Increment(invalidRepo)
			},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := NewRepositoryCounter()

			tc.setup(t, c)

			require.NoError(t, testutil.CollectAndCompare(
				c, buildMetrics(t, tc.expectedMetrics), "gitaly_total_repositories_count"))
		})
	}
}

func buildMetrics(t *testing.T, metrics []metric) *strings.Reader {
	t.Helper()

	var builder strings.Builder
	_, err := builder.WriteString("# HELP gitaly_total_repositories_count Gauge of number of repositories by storage and path\n")
	require.NoError(t, err)
	_, err = builder.WriteString("# TYPE gitaly_total_repositories_count gauge\n")
	require.NoError(t, err)

	for _, item := range metrics {
		_, err = builder.WriteString(fmt.Sprintf("gitaly_total_repositories_count{prefix=%q, storage=%q} %d\n",
			item.prefix, item.storage, item.count))
		require.NoError(t, err)
	}
	return strings.NewReader(builder.String())
}
