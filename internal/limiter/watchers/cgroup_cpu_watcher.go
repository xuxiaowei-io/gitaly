package watchers

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/limiter"
)

const (
	cgroupCPUWatcherName  = "CgroupCpu"
	cpuThrottledThreshold = 0.5
)

// CgroupCPUWatcher implements ResourceWatcher interface for watching CPU throttling of cgroup. Cgroup doesn't have an
// easy way to gauge how bad the CPU is percentage. Hence, this watcher compares the recorded total throttled time
// between two polls. If the throttled time exceeds 50% of the observation window, it returns a backoff event. The
// watcher uses `throttled_time` (CgroupV1) or `throttled_usec` (CgroupV2) stats from the cgroup manager.
type CgroupCPUWatcher struct {
	manager         cgroups.Manager
	lastPoll        time.Time
	lastParentStats cgroups.CgroupStats

	// currentTime is the function that returns the current time. If it's not set, time.Now() is used
	// instead. It's used for tests only.
	currentTime func() time.Time
}

// NewCgroupCPUWatcher is the initializer of CgroupCPUWatcher
func NewCgroupCPUWatcher(manager cgroups.Manager) *CgroupCPUWatcher {
	return &CgroupCPUWatcher{
		manager: manager,
	}
}

// Name returns the name of CgroupCPUWatcher
func (c *CgroupCPUWatcher) Name() string {
	return cgroupCPUWatcherName
}

// Poll asserts the cgroup statistics and returns a backoff event accordingly. The condition when a backoff event is
// returned is described above.
func (c *CgroupCPUWatcher) Poll(ctx context.Context) (*limiter.BackoffEvent, error) {
	if !c.manager.Ready() {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: false,
		}, nil
	}

	stats, err := c.manager.Stats()
	if err != nil {
		return nil, fmt.Errorf("cgroup watcher: poll stats from cgroup manager: %w", err)
	}

	currentPoll := time.Now()
	if c.currentTime != nil {
		currentPoll = c.currentTime()
	}
	parentStats := stats.ParentStats
	defer func() {
		c.lastParentStats = parentStats
		c.lastPoll = currentPoll
	}()

	// First poll, not enough clue to conclude
	if c.lastPoll.IsZero() {
		return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
	}

	// Somehow, cgroup stats are reset. It's usually the consequence of cgroup limits being changed. Alternatively,
	// they can be overridden by another program. Either way, the watcher should update the stats accordingly.
	if parentStats.CPUThrottledCount < c.lastParentStats.CPUThrottledCount ||
		parentStats.CPUThrottledDuration < c.lastParentStats.CPUThrottledDuration {
		return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
	}

	throttledDuration := parentStats.CPUThrottledDuration - c.lastParentStats.CPUThrottledDuration
	timeDiff := currentPoll.Sub(c.lastPoll).Abs().Seconds()

	// If the total throttled duration since the last poll exceeds 50%.
	if timeDiff > 0 && throttledDuration/timeDiff > cpuThrottledThreshold {
		return &limiter.BackoffEvent{
			WatcherName:   c.Name(),
			ShouldBackoff: true,
			Reason:        fmt.Sprintf("cgroup CPU throttled too much: %0.2f/%0.2f seconds", throttledDuration, timeDiff),
		}, nil
	}

	return &limiter.BackoffEvent{WatcherName: c.Name(), ShouldBackoff: false}, nil
}
