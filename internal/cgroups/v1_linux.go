package cgroups

import (
	"fmt"
	"hash/crc32"
	"os"
	"strings"

	"github.com/containerd/cgroups"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

// CGroupV1Manager is the manager for cgroups v1
type CGroupV1Manager struct {
	cfg                         cgroupscfg.Config
	hierarchy                   func() ([]cgroups.Subsystem, error)
	memoryFailedTotal, cpuUsage *prometheus.GaugeVec
	procs, gitCgroups           *prometheus.GaugeVec
	gitWithoutCgroup            prometheus.Counter
	gitCmds                     map[string]cgroupscfg.GitCommand
	gitCgroupPool               chan string
}

func newV1Manager(cfg cgroupscfg.Config) *CGroupV1Manager {
	return &CGroupV1Manager{
		cfg: cfg,
		hierarchy: func() ([]cgroups.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
		memoryFailedTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_failed_total",
				Help: "Number of memory usage hits limits",
			},
			[]string{"path"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_usage",
				Help: "CPU Usage of Cgroup",
			},
			[]string{"path", "type"},
		),
		procs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_procs_total",
				Help: "Total number of procs",
			},
			[]string{"path", "subsystem"},
		),
		gitCgroups: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_git_cgroups",
				Help: "Total number of git cgroups in use",
			},
			[]string{"status"},
		),
		gitWithoutCgroup: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_cgroup_git_without_cgroup_total",
				Help: "Total number of git commands without a cgroup",
			},
		),
		gitCmds:       make(map[string]cgroupscfg.GitCommand),
		gitCgroupPool: make(chan string, cfg.Git.Count),
	}
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (cg *CGroupV1Manager) Setup() error {
	var parentResources specs.LinuxResources

	if cg.cfg.Repositories.CPUShares > 0 {
		parentResources.CPU = &specs.LinuxCPU{
			Shares: &cg.cfg.CPUShares,
		}
	}

	if cg.cfg.Repositories.MemoryBytes > 0 {
		parentResources.Memory = &specs.LinuxMemory{
			Limit: &cg.cfg.MemoryBytes,
		}
	}

	if _, err := cgroups.New(
		cg.hierarchy,
		cgroups.StaticPath(cg.currentProcessCgroup()),
		&parentResources,
	); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}

	var repoResources specs.LinuxResources

	if cg.cfg.Repositories.CPUShares > 0 {
		repoResources.CPU = &specs.LinuxCPU{
			Shares: &cg.cfg.Repositories.CPUShares,
		}
	}

	if cg.cfg.Repositories.MemoryBytes > 0 {
		repoResources.Memory = &specs.LinuxMemory{
			Limit: &cg.cfg.Repositories.MemoryBytes,
		}
	}

	for i := 0; i < int(cg.cfg.Repositories.Count); i++ {
		if _, err := cgroups.New(
			cg.hierarchy,
			cgroups.StaticPath(cg.repoPath(i)),
			&repoResources,
		); err != nil {
			return fmt.Errorf("failed creating repository cgroup: %w", err)
		}
	}

	for i := 0; i < int(cg.cfg.Git.Count); i++ {
		path := cg.gitCommandPath(i)
		if _, err := cgroups.New(
			cg.hierarchy,
			cgroups.StaticPath(path),
			&specs.LinuxResources{},
		); err != nil {
			return fmt.Errorf("failed creating git command cgroup: %w", err)
		}

		cg.gitCgroupPool <- path
	}

	for _, command := range cg.cfg.Git.Commands {
		cg.gitCmds[command.Name] = command
	}

	return nil
}

// AddCommand adds the given command to one of the CGroup's buckets. The bucket used for the command
// is determined by hashing the commands arguments. No error is returned if the command has already
// exited.
func (cg *CGroupV1Manager) AddCommand(
	cmd *command.Command,
	gitCmdName string,
	repo repository.GitRepo,
) error {
	logger := log.Default().WithField("component", "cgroups").WithField("git_command", gitCmdName)

	gitCmdDetails, ok := cg.gitCmds[gitCmdName]
	if !ok {
		logger.Info("adding to repository cgroup")
		return cg.addRepositoryCmd(cmd, repo)
	}

	select {
	case cgroupPath := <-cg.gitCgroupPool:
		cmd.SetCgroupPath(cgroupPath)
		cmd.SetCgroupReleaseFunc(func() {
			cg.gitCgroupPool <- cgroupPath
		})

		control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(cgroupPath))
		if err != nil {
			return fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
		}

		var resources specs.LinuxResources

		if gitCmdDetails.CPUShares > 0 {
			resources.CPU = &specs.LinuxCPU{
				Shares: &gitCmdDetails.CPUShares,
			}
		}

		if gitCmdDetails.MemoryBytes > 0 {
			resources.Memory = &specs.LinuxMemory{
				Limit: &gitCmdDetails.MemoryBytes,
			}
		}

		if err := control.Update(&resources); err != nil {
			return fmt.Errorf("failed updating %s cgroups: %w", cgroupPath, err)
		}

		if err := control.Add(cgroups.Process{Pid: cmd.Pid()}); err != nil {
			// Command could finish so quickly before we can add it to a cgroup, so
			// we don't consider it an error.
			if strings.Contains(err.Error(), "no such process") {
				return nil
			}
			return fmt.Errorf("failed adding process to cgroup: %w", err)
		}
		logger.Info("adding to git cgroup")
	default:
		cg.gitWithoutCgroup.Inc()
	}
	return nil
}

func (cg *CGroupV1Manager) addRepositoryCmd(cmd *command.Command, repo repository.GitRepo) error {
	var data []byte
	if repo == nil {
		data = []byte(strings.Join(cmd.Args(), ""))
	} else {
		data = []byte(strings.Join([]string{repo.GetStorageName(), repo.GetRelativePath()}, ""))

	}

	checksum := crc32.ChecksumIEEE(data)
	groupID := uint(checksum) % cg.cfg.Repositories.Count
	cgroupPath := cg.repoPath(int(groupID))

	cmd.SetCgroupPath(cgroupPath)

	return cg.addToCgroup(cmd.Pid(), cgroupPath)
}

func (cg *CGroupV1Manager) addToCgroup(pid int, cgroupPath string) error {
	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(cgroupPath))
	if err != nil {
		return fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}

	if err := control.Add(cgroups.Process{Pid: pid}); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

// Collect collects metrics from the cgroups controller
func (cg *CGroupV1Manager) Collect(ch chan<- prometheus.Metric) {
	path := cg.currentProcessCgroup()
	logger := log.Default().WithField("cgroup_path", path)
	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(path))
	if err != nil {
		logger.WithError(err).Warn("unable to load cgroup controller")
		return
	}

	if metrics, err := control.Stat(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup stats")
	} else {
		memoryMetric := cg.memoryFailedTotal.WithLabelValues(path)
		memoryMetric.Set(float64(metrics.Memory.Usage.Failcnt))
		ch <- memoryMetric

		cpuUserMetric := cg.cpuUsage.WithLabelValues(path, "user")
		cpuUserMetric.Set(float64(metrics.CPU.Usage.User))
		ch <- cpuUserMetric

		cpuKernelMetric := cg.cpuUsage.WithLabelValues(path, "kernel")
		cpuKernelMetric.Set(float64(metrics.CPU.Usage.Kernel))
		ch <- cpuKernelMetric
	}

	if subsystems, err := cg.hierarchy(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup hierarchy")
	} else {
		for _, subsystem := range subsystems {
			processes, err := control.Processes(subsystem.Name(), true)
			if err != nil {
				logger.WithField("subsystem", subsystem.Name()).
					WithError(err).
					Warn("unable to get process list")
				continue
			}

			procsMetric := cg.procs.WithLabelValues(path, string(subsystem.Name()))
			procsMetric.Set(float64(len(processes)))
			ch <- procsMetric
		}
	}

	gitCgroupsTotalMetric := cg.gitCgroups.WithLabelValues("total")
	gitCgroupsTotalMetric.Set(float64(cg.cfg.Git.Count))
	ch <- gitCgroupsTotalMetric

	gitCgroupsInUseMetric := cg.gitCgroups.WithLabelValues("in_use")
	gitCgroupsInUseMetric.Set(float64(int(cg.cfg.Git.Count) - len(cg.gitCgroupPool)))
	ch <- gitCgroupsInUseMetric
	ch <- cg.gitWithoutCgroup
}

// Describe describes the cgroup metrics that Collect provides
func (cg *CGroupV1Manager) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(cg, ch)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (cg *CGroupV1Manager) Cleanup() error {
	processCgroupPath := cg.currentProcessCgroup()

	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(processCgroupPath))
	if err != nil {
		return fmt.Errorf("failed loading cgroup %s: %w", processCgroupPath, err)
	}

	if err := control.Delete(); err != nil {
		return fmt.Errorf("failed cleaning up cgroup %s: %w", processCgroupPath, err)
	}

	return nil
}

func (cg *CGroupV1Manager) repoPath(groupID int) string {
	return fmt.Sprintf("/%s/repos-%d", cg.currentProcessCgroup(), groupID)
}

func (cg *CGroupV1Manager) gitCommandPath(groupID int) string {
	return fmt.Sprintf("/%s/git-commands-%d", cg.currentProcessCgroup(), groupID)
}

func (cg *CGroupV1Manager) currentProcessCgroup() string {
	return fmt.Sprintf("/%s/gitaly-%d", cg.cfg.HierarchyRoot, os.Getpid())
}

func defaultSubsystems(root string) ([]cgroups.Subsystem, error) {
	subsystems := []cgroups.Subsystem{
		cgroups.NewMemory(root, cgroups.OptionalSwap()),
		cgroups.NewCpu(root),
	}

	return subsystems, nil
}
