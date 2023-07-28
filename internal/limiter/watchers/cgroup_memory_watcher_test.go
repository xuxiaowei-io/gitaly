package watchers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestCgroupMemoryWatcher_Name(t *testing.T) {
	t.Parallel()

	manager := NewCgroupMemoryWatcher(&testCgroupManager{})
	require.Equal(t, cgroupMemoryWatcherName, manager.Name())
}

func TestCgroupMemoryWatcher_Poll(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		manager       *testCgroupManager
		expectedEvent *limiter.BackoffEvent
		expectedErr   error
	}{
		{
			desc:    "disabled watcher",
			manager: &testCgroupManager{ready: false},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup stats return empty stats",
			manager: &testCgroupManager{
				ready:     true,
				statsList: []cgroups.Stats{{}},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
		},
		{
			desc: "cgroup stats query returns errors",
			manager: &testCgroupManager{
				ready:     true,
				statsErr:  fmt.Errorf("something goes wrong"),
				statsList: []cgroups.Stats{{}},
			},
			expectedErr: fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", fmt.Errorf("something goes wrong")),
		},
		{
			desc: "cgroup memory usage is more than 90%",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1800000000,
							MemoryLimit: 2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup memory exceeds limit: 1800000000/2000000000",
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup is under OOM",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1900000000,
							MemoryLimit: 2000000000,
							UnderOOM:    true,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: true,
				Reason:        "cgroup is under OOM",
			},
			expectedErr: nil,
		},
		{
			desc: "cgroup memory usage normal",
			manager: &testCgroupManager{
				ready: true,
				statsList: []cgroups.Stats{
					{
						ParentStats: cgroups.CgroupStats{
							MemoryUsage: 1700000000,
							MemoryLimit: 2000000000,
						},
					},
				},
			},
			expectedEvent: &limiter.BackoffEvent{
				WatcherName:   cgroupMemoryWatcherName,
				ShouldBackoff: false,
			},
			expectedErr: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			watcher := NewCgroupMemoryWatcher(tc.manager)
			event, err := watcher.Poll(testhelper.Context(t))

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				require.Nil(t, event)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedEvent, event)
			}
		})
	}
}
