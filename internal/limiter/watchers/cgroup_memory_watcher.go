package watchers

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
)

const (
	cgroupMemoryWatcherName = "CgroupMemory"
	memoryThreshold         = 0.9
)

// CgroupMemoryWatcher implements ResourceWatcher interface. This watcher polls
// the statistics from the cgroup manager. It returns a backoff event in two
// conditions:
// * The current memory usage exceeds a soft threshold (90%).
// * The cgroup is under OOM.
type CgroupMemoryWatcher struct {
	manager cgroups.Manager
}

// NewCgroupMemoryWatcher is the initializer of CgroupMemoryWatcher
func NewCgroupMemoryWatcher(manager cgroups.Manager) *CgroupMemoryWatcher {
	return &CgroupMemoryWatcher{
		manager: manager,
	}
}

// Name returns the name of CgroupMemoryWatcher
func (c *CgroupMemoryWatcher) Name() string {
	return cgroupMemoryWatcherName
}

// Poll asserts the cgroup statistics and returns a backoff event accordingly
// when it is triggered. These stats are fetched from cgroup manager.
func (c *CgroupMemoryWatcher) Poll(context.Context) (*limiter.BackoffEvent, error) {
	if !c.manager.Ready() {
		return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
	}

	stats, err := c.manager.Stats()
	if err != nil {
		return nil, fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", err)
	}
	parentStats := stats.ParentStats

	// Whether the parent cgroup isthe memory cgroup is under OOM, tasks may be stopped. This stat is available in
	// Cgroup V1 only.
	if parentStats.UnderOOM {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        "cgroup is under OOM",
		}, nil
	}

	if parentStats.MemoryLimit > 0 && parentStats.MemoryUsage > 0 &&
		float64(parentStats.MemoryUsage)/float64(parentStats.MemoryLimit) >= memoryThreshold {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        fmt.Sprintf("cgroup memory exceeds limit: %d/%d", parentStats.MemoryUsage, parentStats.MemoryLimit),
		}, nil
	}

	return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
}
