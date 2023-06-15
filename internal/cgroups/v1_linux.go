//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/cgroups/v3/cgroup1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

type cgroupV1Handler struct {
	cfg                        cgroupscfg.Config
	hierarchy                  func() ([]cgroup1.Subsystem, error)
	memoryReclaimAttemptsTotal *prometheus.GaugeVec
	cpuUsage                   *prometheus.GaugeVec
	cpuCFSPeriods              *prometheus.Desc
	cpuCFSThrottledPeriods     *prometheus.Desc
	cpuCFSThrottledTime        *prometheus.Desc
	procs                      *prometheus.GaugeVec
	pid                        int
}

func newV1Handler(cfg cgroupscfg.Config, pid int) *cgroupV1Handler {
	return &cgroupV1Handler{
		cfg: cfg,
		pid: pid,
		hierarchy: func() ([]cgroup1.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
		memoryReclaimAttemptsTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_reclaim_attempts_total",
				Help: "Number of memory usage hits limits",
			},
			[]string{"path"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_usage_total",
				Help: "CPU Usage of Cgroup",
			},
			[]string{"path", "type"},
		),
		cpuCFSPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_periods_total",
			"Number of elapsed enforcement period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledPeriods: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_periods_total",
			"Number of throttled period intervals",
			[]string{"path"}, nil,
		),
		cpuCFSThrottledTime: prometheus.NewDesc(
			"gitaly_cgroup_cpu_cfs_throttled_seconds_total",
			"Total time duration the Cgroup has been throttled",
			[]string{"path"}, nil,
		),
		procs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_procs_total",
				Help: "Total number of procs",
			},
			[]string{"path", "subsystem"},
		),
	}
}

func (cvh *cgroupV1Handler) setupParent(parentResources *specs.LinuxResources) error {
	if _, err := cgroup1.New(
		cgroup1.StaticPath(cvh.currentProcessCgroup()),
		parentResources,
		cgroup1.WithHiearchy(cvh.hierarchy),
	); err != nil {
		return fmt.Errorf("failed creating parent cgroup: %w", err)
	}
	return nil
}

func (cvh *cgroupV1Handler) setupRepository(reposResources *specs.LinuxResources) error {
	for i := 0; i < int(cvh.cfg.Repositories.Count); i++ {
		if _, err := cgroup1.New(
			cgroup1.StaticPath(cvh.repoPath(i)),
			reposResources,
			cgroup1.WithHiearchy(cvh.hierarchy),
		); err != nil {
			return fmt.Errorf("failed creating repository cgroup: %w", err)
		}
	}
	return nil
}

func (cvh *cgroupV1Handler) addToCgroup(pid int, cgroupPath string) error {
	control, err := cgroup1.Load(
		cgroup1.StaticPath(cgroupPath),
		cgroup1.WithHiearchy(cvh.hierarchy),
	)
	if err != nil {
		return fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}

	if err := control.Add(cgroup1.Process{Pid: pid}); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cvh *cgroupV1Handler) collect(ch chan<- prometheus.Metric) {
	if !cvh.cfg.MetricsEnabled {
		return
	}

	for i := 0; i < int(cvh.cfg.Repositories.Count); i++ {
		repoPath := cvh.repoPath(i)
		logger := log.Default().WithField("cgroup_path", repoPath)
		control, err := cgroup1.Load(
			cgroup1.StaticPath(repoPath),
			cgroup1.WithHiearchy(cvh.hierarchy),
		)
		if err != nil {
			logger.WithError(err).Warn("unable to load cgroup controller")
			return
		}

		if metrics, err := control.Stat(); err != nil {
			logger.WithError(err).Warn("unable to get cgroup stats")
		} else {
			memoryMetric := cvh.memoryReclaimAttemptsTotal.WithLabelValues(repoPath)
			memoryMetric.Set(float64(metrics.Memory.Usage.Failcnt))
			ch <- memoryMetric

			cpuUserMetric := cvh.cpuUsage.WithLabelValues(repoPath, "user")
			cpuUserMetric.Set(float64(metrics.CPU.Usage.User))
			ch <- cpuUserMetric

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSPeriods,
				prometheus.CounterValue,
				float64(metrics.CPU.Throttling.Periods),
				repoPath,
			)

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSThrottledPeriods,
				prometheus.CounterValue,
				float64(metrics.CPU.Throttling.ThrottledPeriods),
				repoPath,
			)

			ch <- prometheus.MustNewConstMetric(
				cvh.cpuCFSThrottledTime,
				prometheus.CounterValue,
				float64(metrics.CPU.Throttling.ThrottledTime)/float64(time.Second),
				repoPath,
			)

			cpuKernelMetric := cvh.cpuUsage.WithLabelValues(repoPath, "kernel")
			cpuKernelMetric.Set(float64(metrics.CPU.Usage.Kernel))
			ch <- cpuKernelMetric
		}

		if subsystems, err := cvh.hierarchy(); err != nil {
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

				procsMetric := cvh.procs.WithLabelValues(repoPath, string(subsystem.Name()))
				procsMetric.Set(float64(len(processes)))
				ch <- procsMetric
			}
		}
	}
}

func (cvh *cgroupV1Handler) cleanup() error {
	processCgroupPath := cvh.currentProcessCgroup()

	control, err := cgroup1.Load(
		cgroup1.StaticPath(processCgroupPath),
		cgroup1.WithHiearchy(cvh.hierarchy),
	)
	if err != nil {
		return fmt.Errorf("failed loading cgroup %s: %w", processCgroupPath, err)
	}

	if err := control.Delete(); err != nil {
		return fmt.Errorf("failed cleaning up cgroup %s: %w", processCgroupPath, err)
	}

	return nil
}

func (cvh *cgroupV1Handler) repoPath(groupID int) string {
	return filepath.Join(cvh.currentProcessCgroup(), fmt.Sprintf("repos-%d", groupID))
}

func (cvh *cgroupV1Handler) currentProcessCgroup() string {
	return config.GetGitalyProcessTempDir(cvh.cfg.HierarchyRoot, cvh.pid)
}

func defaultSubsystems(root string) ([]cgroup1.Subsystem, error) {
	subsystems := []cgroup1.Subsystem{
		cgroup1.NewMemory(root, cgroup1.OptionalSwap()),
		cgroup1.NewCpu(root),
	}

	return subsystems, nil
}

func pruneOldCgroupsV1(cfg cgroupscfg.Config, logger logrus.FieldLogger) {
	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, "memory",
			cfg.HierarchyRoot),
	); err != nil {
		logger.WithError(err).Error("failed to clean up memory cgroups")
	}

	if err := config.PruneOldGitalyProcessDirectories(
		logger,
		filepath.Join(cfg.Mountpoint, "cpu",
			cfg.HierarchyRoot),
	); err != nil {
		logger.WithError(err).Error("failed to clean up cpu cgroups")
	}
}
