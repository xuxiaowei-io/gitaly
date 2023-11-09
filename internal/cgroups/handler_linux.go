//go:build linux

package cgroups

import (
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/opencontainers/runtime-spec/specs-go"
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