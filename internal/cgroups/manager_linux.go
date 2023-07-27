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
	log "github.com/sirupsen/logrus"
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
	repoPath(groupID int) string
}

// CGroupManager is a manager class that implements specific methods related to cgroups
type CGroupManager struct {
	cfg cgroupscfg.Config
	pid int

	handler cgroupHandler
}

func newCgroupManager(cfg cgroupscfg.Config, pid int) *CGroupManager {
	return newCgroupManagerWithMode(cfg, pid, cgrps.Mode())
}

func newCgroupManagerWithMode(cfg cgroupscfg.Config, pid int, mode cgrps.CGMode) *CGroupManager {
	var handler cgroupHandler
	switch mode {
	case cgrps.Legacy, cgrps.Hybrid:
		handler = newV1Handler(cfg, pid)
	case cgrps.Unified:
		handler = newV2Handler(cfg, pid)
		log.Warnf("Gitaly now includes experimental support for CgroupV2. Please proceed with caution and use this experimental feature at your own risk")
	default:
		log.Warnf("Gitaly has encountered an issue while trying to detect the version of the system's cgroup. As a result, all subsequent commands will be executed without cgroup support. Please check the system's cgroup configuration and try again")
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
	return nil
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

func pruneOldCgroups(cfg cgroupscfg.Config, logger log.FieldLogger) {
	pruneOldCgroupsWithMode(cfg, logger, cgrps.Mode())
}

func pruneOldCgroupsWithMode(cfg cgroupscfg.Config, logger log.FieldLogger, mode cgrps.CGMode) {
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
