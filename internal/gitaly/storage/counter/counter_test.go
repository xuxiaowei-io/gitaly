package counter

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

type metric struct {
	storage, prefix string
	count           int
}

func TestCountStorages(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		storageNames    []string
		repos           []string
		files           []string
		expectedMetrics []metric
	}{
		{
			name:         "non-praefect paths",
			storageNames: []string{"foo"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@hashed/01/23/repo-2.git"},
			expectedMetrics: []metric{
				{storage: "foo", prefix: "@hashed", count: 2},
			},
		},
		{
			name:         "multiple prefixes",
			storageNames: []string{"foo"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@snippets/01/23/repo-2.git", "@groups/ee/ff/wiki.git"},
			expectedMetrics: []metric{
				{storage: "foo", prefix: "@hashed", count: 1},
				{storage: "foo", prefix: "@snippets", count: 1},
				{storage: "foo", prefix: "@groups", count: 1},
			},
		},
		{
			name:         "@cluster paths",
			storageNames: []string{"foo"},
			repos:        []string{"@cluster/bar/01/23/1234", "@cluster/baz/45/67/89ab"},
			expectedMetrics: []metric{
				{storage: "foo", prefix: "bar", count: 1},
				{storage: "foo", prefix: "baz", count: 1},
			},
		},
		{
			name:         "multiple storages",
			storageNames: []string{"foo", "bar"},
			repos:        []string{"@hashed/aa/bb/repo-1.git", "@snippets/01/23/repo-2.git"},
			expectedMetrics: []metric{
				{storage: "foo", prefix: "@hashed", count: 1},
				{storage: "foo", prefix: "@snippets", count: 1},
				{storage: "bar", prefix: "@hashed", count: 1},
				{storage: "bar", prefix: "@snippets", count: 1},
			},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg := testcfg.Build(t, testcfg.WithStorages(tc.storageNames[0], tc.storageNames[1:]...))
			locator := config.NewLocator(cfg)
			logger := testhelper.NewDiscardingLogger(t)

			for _, storage := range cfg.Storages {
				for _, path := range tc.repos {
					_, _ = gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
						Storage:                storage,
						RelativePath:           path,
					})
				}
			}

			c := NewRepositoryCounter()

			c.countRepositories(ctx, locator, cfg.Storages, logger)

			require.NoError(t, testutil.CollectAndCompare(
				c, buildMetrics(t, tc.expectedMetrics), "gitaly_total_repositories_count"))
		})
	}
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
		suppress        bool
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
		{
			name: "suppress",
			setup: func(t *testing.T, c *RepositoryCounter) {
				c.Increment(repo)
			},
			suppress: true,
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := NewRepositoryCounter()

			c.suppressMetric.Store(tc.suppress)
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
