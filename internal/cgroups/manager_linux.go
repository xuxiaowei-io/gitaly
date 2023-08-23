//go:build linux

package cgroups

import (
	"fmt"
	"hash/crc32"
	"os/exec"
	"strings"

	cgrps "github.com/containerd/cgroups/v3"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

// cfs_period_us hardcoded to be 100ms.
const cfsPeriodUs uint64 = 100000

type cgroupHandler interface {
	setupParent(reposResources *specs.LinuxResources) error
	setupRepository(reposResources *specs.LinuxResources) error
	addToCgroup(pid int, cgroupPath string) error
	collect(ch chan<- prometheus.Metric)
	cleanup() error
	currentProcessCgroup() string
	mainPath() string
	repoPath(groupID int) string
	stats() (Stats, error)
}

// CGroupManager is a manager class that implements specific methods related to cgroups
type CGroupManager struct {
	cfg     cgroupscfg.Config
	pid     int
	enabled bool

	handler cgroupHandler
}

func newCgroupManager(cfg cgroupscfg.Config, logger logrus.FieldLogger, pid int) *CGroupManager {
	return newCgroupManagerWithMode(cfg, logger, pid, cgrps.Mode())
}

func newCgroupManagerWithMode(cfg cgroupscfg.Config, logger logrus.FieldLogger, pid int, mode cgrps.CGMode) *CGroupManager {
	var handler cgroupHandler
	switch mode {
	case cgrps.Legacy, cgrps.Hybrid:
		handler = newV1Handler(cfg, logger, pid)
	case cgrps.Unified:
		handler = newV2Handler(cfg, logger, pid)
		logger.Warnf("Gitaly now includes experimental support for CgroupV2. Please proceed with caution and use this experimental feature at your own risk")
	default:
		logger.Warnf("Gitaly has encountered an issue while trying to detect the version of the system's cgroup. As a result, all subsequent commands will be executed without cgroup support. Please check the system's cgroup configuration and try again")
		return nil
	}

	return &CGroupManager{
		cfg:     cfg,
		pid:     pid,
		handler: handler,
	}
}

// Setup parent cgroups and repository sub cgroups
func (cgm *CGroupManager) Setup() error {
	if err := cgm.handler.setupParent(cgm.configParentResources()); err != nil {
		return err
	}
	if err := cgm.handler.setupRepository(cgm.configRepositoryResources()); err != nil {
		return err
	}
	// When the IncludeGitalyProcess configuration is enabled, the main cgroup process's PID is added to Gitaly's
	// cgroup hierarchy. The process doesn't possess its own exclusive limit. Rather, it shares the limit with all
	// spawned Git processes, which are also managed by the repository cgroup, under the umbrella of the parent
	// cgroup. This configuration offers superior protection and flexibility compared to setting a fixed limit
	// specifically for the Gitaly process:
	// - If certain Git commands monopolize resources, they will be terminated by the repository cgroup.
	// - If the cumulative resource usage of all Git commands surpasses the parent cgroup limit, the Out-Of-Memory
	// (OOM) killer is triggered which terminates the most resource-intensive processes. Resource-heavy Git commands
	// are more likely to be targeted.
	// - If the Gitaly process consumes excessive memory (possibly due to a memory leak), it is allowed to do so if
	// the parent cgroup has available resources. However, if the parent cgroup's limit is reached, the Gitaly
	// process will be terminated.
	// Consequently, the Gitaly process typically has more resource leeway than the Git processes and is likely to
	// be terminated last in the event of a problem.
	//
	// Example cgroup hierarchy if this config is enabled (Cgroup V2):
	// /sys/fs/cgroup/gitaly/gitaly-121843
	// |_ main
	// |_ repos-0
	// |_ repos-1
	// |_ ...
	if cgm.cfg.IncludeGitalyProcess {
		if err := cgm.handler.addToCgroup(cgm.pid, cgm.handler.mainPath()); err != nil {
			return err
		}
	}
	cgm.enabled = true

	return nil
}

// Ready returns true if the Cgroup manager is configured and ready to use.
func (cgm *CGroupManager) Ready() bool {
	return cgm.enabled
}

// AddCommand adds a Cmd to a cgroup
func (cgm *CGroupManager) AddCommand(cmd *exec.Cmd, opts ...AddCommandOption) (string, error) {
	var cfg addCommandCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	key := cfg.cgroupKey
	if key == "" {
		key = strings.Join(cmd.Args, "/")
	}

	checksum := crc32.ChecksumIEEE(
		[]byte(key),
	)

	if cmd.Process == nil {
		return "", fmt.Errorf("cannot add command that has not yet been started")
	}

	groupID := uint(checksum) % cgm.cfg.Repositories.Count
	cgroupPath := cgm.handler.repoPath(int(groupID))

	return cgroupPath, cgm.handler.addToCgroup(cmd.Process.Pid, cgroupPath)
}

// Cleanup cleans up cgroups created in Setup.
func (cgm *CGroupManager) Cleanup() error {
	return cgm.handler.cleanup()
}

// Describe is used to generate description information for each CGroupManager prometheus metric
func (cgm *CGroupManager) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(cgm, ch)
}

// Collect is used to collect the current values of all CGroupManager prometheus metrics
func (cgm *CGroupManager) Collect(ch chan<- prometheus.Metric) {
	cgm.handler.collect(ch)
}

// Stats returns cgroup accounting statistics collected by reading
// cgroupfs files.
func (cgm *CGroupManager) Stats() (Stats, error) {
	return cgm.handler.stats()
}

func (cgm *CGroupManager) currentProcessCgroup() string {
	return cgm.handler.currentProcessCgroup()
}

func (cgm *CGroupManager) configParentResources() *specs.LinuxResources {
	cfsPeriodUs := cfsPeriodUs
	var parentResources specs.LinuxResources
	// Leave them `nil` so it takes kernel default unless cfg value above `0`.
	parentResources.CPU = &specs.LinuxCPU{}

	if cgm.cfg.CPUShares > 0 {
		parentResources.CPU.Shares = &cgm.cfg.CPUShares
	}

	if cgm.cfg.CPUQuotaUs > 0 {
		parentResources.CPU.Quota = &cgm.cfg.CPUQuotaUs
		parentResources.CPU.Period = &cfsPeriodUs
	}

	if cgm.cfg.MemoryBytes > 0 {
		parentResources.Memory = &specs.LinuxMemory{Limit: &cgm.cfg.MemoryBytes}
	}
	return &parentResources
}

func (cgm *CGroupManager) configRepositoryResources() *specs.LinuxResources {
	cfsPeriodUs := cfsPeriodUs
	var reposResources specs.LinuxResources
	// Leave them `nil` so it takes kernel default unless cfg value above `0`.
	reposResources.CPU = &specs.LinuxCPU{}

	if cgm.cfg.Repositories.CPUShares > 0 {
		reposResources.CPU.Shares = &cgm.cfg.Repositories.CPUShares
	}

	if cgm.cfg.Repositories.CPUQuotaUs > 0 {
		reposResources.CPU.Quota = &cgm.cfg.Repositories.CPUQuotaUs
		reposResources.CPU.Period = &cfsPeriodUs
	}

	if cgm.cfg.Repositories.MemoryBytes > 0 {
		reposResources.Memory = &specs.LinuxMemory{Limit: &cgm.cfg.Repositories.MemoryBytes}
	}
	return &reposResources
}

func pruneOldCgroups(cfg cgroupscfg.Config, logger logrus.FieldLogger) {
	pruneOldCgroupsWithMode(cfg, logger, cgrps.Mode())
}

func pruneOldCgroupsWithMode(cfg cgroupscfg.Config, logger logrus.FieldLogger, mode cgrps.CGMode) {
	if cfg.HierarchyRoot == "" {
		return
	}

	switch mode {
	case cgrps.Legacy, cgrps.Hybrid:
		pruneOldCgroupsV1(cfg, logger)
	case cgrps.Unified:
		pruneOldCgroupsV2(cfg, logger)
	default:
	}
}
