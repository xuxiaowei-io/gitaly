//go:build linux

package cgroups

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/opencontainers/runtime-spec/specs-go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type (
	// createCgroupFunc is a function that creates a new cgroup.
	createCgroupFunc[T any, H any] func(hierarchy H, resources *specs.LinuxResources, path string) (T, error)
	// loadCgroupFunc is a function that loads an existing cgroup.
	loadCgroupFunc[T any, H any] func(hierarchy H, path string) (T, error)
	// addToCgroupFunc is a function that adds a process to an existing cgroup.
	addToCgroupFunc[T any] func(control T, pid int) error
	// deleteCgroupFunc is a function that deletes a cgroup.
	deleteCgroupFunc[T any] func(control T) error
)

// genericHandler is a cgroup handler that can be instantiated for either cgroups-v1
// or cgroups-v2.
type genericHandler[T any, H any] struct {
	cfg           cgroupscfg.Config
	logger        log.Logger
	pid           int
	supportsClone bool

	// hierarchy is either a cgroup1.Hierarchy or the cgroup2 Mountpoint path.
	hierarchy  H
	createFunc createCgroupFunc[T, H]
	loadFunc   loadCgroupFunc[T, H]
	addFunc    addToCgroupFunc[T]
	deleteFunc deleteCgroupFunc[T]

	metrics *cgroupsMetrics
}

func newV1GenericHandler(
	cfg cgroupscfg.Config,
	logger log.Logger,
	pid int,
) *genericHandler[cgroup1.Cgroup, cgroup1.Hierarchy] {
	return &genericHandler[cgroup1.Cgroup, cgroup1.Hierarchy]{
		cfg:           cfg,
		logger:        logger,
		pid:           pid,
		supportsClone: false,
		hierarchy: func() ([]cgroup1.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
		metrics: newV1CgroupsMetrics(),
		createFunc: func(hierarchy cgroup1.Hierarchy, resources *specs.LinuxResources, cgroupPath string) (cgroup1.Cgroup, error) {
			return cgroup1.New(
				cgroup1.StaticPath(cgroupPath),
				resources,
				cgroup1.WithHiearchy(hierarchy))
		},
		loadFunc: func(hierarchy cgroup1.Hierarchy, cgroupPath string) (cgroup1.Cgroup, error) {
			return cgroup1.Load(
				cgroup1.StaticPath(cgroupPath),
				cgroup1.WithHiearchy(hierarchy),
			)
		},
		addFunc: func(control cgroup1.Cgroup, pid int) error {
			return control.Add(cgroup1.Process{Pid: pid})
		},
		deleteFunc: func(control cgroup1.Cgroup) error {
			return control.Delete()
		},
	}
}

func newV2GenericHandler(
	cfg cgroupscfg.Config,
	logger log.Logger,
	pid int,
) *genericHandler[*cgroup2.Manager, string] {
	return &genericHandler[*cgroup2.Manager, string]{
		cfg:           cfg,
		logger:        logger,
		pid:           pid,
		supportsClone: true,
		hierarchy:     cfg.Mountpoint,
		metrics:       newV2CgroupsMetrics(),
		createFunc: func(mountpoint string, resources *specs.LinuxResources, cgroupPath string) (*cgroup2.Manager, error) {
			return cgroup2.NewManager(
				mountpoint,
				"/"+cgroupPath,
				cgroup2.ToResources(resources),
			)
		},
		loadFunc: func(mountpoint string, cgroupPath string) (*cgroup2.Manager, error) {
			return cgroup2.Load("/"+cgroupPath, cgroup2.WithMountpoint(mountpoint))
		},
		addFunc: func(control *cgroup2.Manager, pid int) error {
			return control.AddProc(uint64(pid))
		},
		deleteFunc: func(control *cgroup2.Manager) error {
			return control.Delete()
		},
	}
}

func (cvh *genericHandler[T, H]) currentProcessCgroup() string {
	return config.GetGitalyProcessTempDir(cvh.cfg.HierarchyRoot, cvh.pid)
}

func (cvh *genericHandler[T, H]) createCgroup(repoResources *specs.LinuxResources, cgroupPath string) error {
	_, err := cvh.createFunc(cvh.hierarchy, repoResources, cgroupPath)
	return err
}

func (cvh *genericHandler[T, H]) addToCgroup(pid int, cgroupPath string) error {
	control, err := cvh.loadFunc(cvh.hierarchy, cgroupPath)
	if err != nil {
		return err
	}

	if err := cvh.addFunc(control, pid); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cvh *genericHandler[T, H]) setupParent(parentResources *specs.LinuxResources) error {
	if _, err := cvh.createFunc(cvh.hierarchy, parentResources, cvh.currentProcessCgroup()); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}
	return nil
}

func (cvh *genericHandler[T, H]) cleanup() error {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cvh.loadFunc(cvh.hierarchy, processCgroupPath)
	if err != nil {
		return err
	}

	if err := cvh.deleteFunc(control); err != nil {
		return fmt.Errorf("failed cleaning up cgroup %s: %w", processCgroupPath, err)
	}

	return nil
}

func (cvh *genericHandler[T, H]) repoPath(groupID int) string {
	return filepath.Join(cvh.currentProcessCgroup(), fmt.Sprintf("repos-%d", groupID))
}

func (cvh *genericHandler[T, H]) supportsCloneIntoCgroup() bool {
	return cvh.supportsClone
}

func (cvh *genericHandler[T, H]) stats() (Stats, error) {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cvh.loadFunc(cvh.hierarchy, processCgroupPath)
	if err != nil {
		return Stats{}, err
	}

	switch c := any(control).(type) {
	case cgroup1.Cgroup:
		return v1Stats(c, processCgroupPath)
	case *cgroup2.Manager:
		return v2stats(c, processCgroupPath)
	default:
		return Stats{}, errors.New("unknown cgroup type")
	}
}

func v1Stats(control cgroup1.Cgroup, processCgroupPath string) (Stats, error) {
	metrics, err := control.Stat()
	if err != nil {
		return Stats{}, fmt.Errorf("failed to fetch metrics %s: %w", processCgroupPath, err)
	}

	return Stats{
		ParentStats: CgroupStats{
			CPUThrottledCount:    metrics.CPU.Throttling.ThrottledPeriods,
			CPUThrottledDuration: float64(metrics.CPU.Throttling.ThrottledTime) / float64(time.Second),
			MemoryUsage:          metrics.Memory.Usage.Usage,
			MemoryLimit:          metrics.Memory.Usage.Limit,
			OOMKills:             metrics.MemoryOomControl.OomKill,
			UnderOOM:             metrics.MemoryOomControl.UnderOom != 0,
		},
	}, nil
}

func v2stats(control *cgroup2.Manager, processCgroupPath string) (Stats, error) {
	metrics, err := control.Stat()
	if err != nil {
		return Stats{}, fmt.Errorf("failed to fetch metrics %s: %w", processCgroupPath, err)
	}

	stats := Stats{
		ParentStats: CgroupStats{
			CPUThrottledCount:    metrics.CPU.NrThrottled,
			CPUThrottledDuration: float64(metrics.CPU.ThrottledUsec) / float64(time.Second),
			MemoryUsage:          metrics.Memory.Usage,
			MemoryLimit:          metrics.Memory.UsageLimit,
		},
	}
	if metrics.MemoryEvents != nil {
		stats.ParentStats.OOMKills = metrics.MemoryEvents.OomKill
	}
	return stats, nil
}
