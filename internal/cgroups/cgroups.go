package cgroups

import (
	"os/exec"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/cgroups"
)

type addCommandCfg struct {
	cgroupKey string
}

// AddCommandOption is an option that can be passed to AddCommand.
type AddCommandOption func(*addCommandCfg)

// WithCgroupKey overrides the key used to derive the Cgroup bucket. If not passed, then the command
// arguments will be used as the cgroup key.
func WithCgroupKey(cgroupKey string) AddCommandOption {
	return func(cfg *addCommandCfg) {
		cfg.cgroupKey = cgroupKey
	}
}

// Manager supplies an interface for interacting with cgroups
type Manager interface {
	// Setup creates cgroups and assigns configured limitations.
	// It is expected to be called once at Gitaly startup from any
	// instance of the Manager.
	Setup() error
	// AddCommand adds a Cmd to a cgroup.
	AddCommand(*exec.Cmd, ...AddCommandOption) (string, error)
	// Cleanup cleans up cgroups created in Setup.
	// It is expected to be called once at Gitaly shutdown from any
	// instance of the Manager.
	Cleanup() error
	Describe(ch chan<- *prometheus.Desc)
	Collect(ch chan<- prometheus.Metric)
}

// NewManager returns the appropriate Cgroups manager
func NewManager(cfg cgroups.Config, pid int) Manager {
	if cfg.Repositories.Count > 0 {
		return newV1Manager(cfg, pid)
	}

	return &NoopManager{}
}

// PruneOldCgroups prunes old cgroups for both the memory and cpu subsystems
func PruneOldCgroups(cfg cgroups.Config, logger log.FieldLogger) {
	if cfg.HierarchyRoot == "" {
		return
	}

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
