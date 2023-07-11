package cgroups

import "github.com/prometheus/client_golang/prometheus"

type cgroupsMetrics struct {
	memoryReclaimAttemptsTotal *prometheus.GaugeVec
	cpuUsage                   *prometheus.GaugeVec
	cpuCFSPeriods              *prometheus.Desc
	cpuCFSThrottledPeriods     *prometheus.Desc
	cpuCFSThrottledTime        *prometheus.Desc
	procs                      *prometheus.GaugeVec
}

func newV1CgroupsMetrics() *cgroupsMetrics {
	return &cgroupsMetrics{
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

func newV2CgroupsMetrics() *cgroupsMetrics {
	return &cgroupsMetrics{
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
