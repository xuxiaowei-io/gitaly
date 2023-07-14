//go:build !linux

package cgroups

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

func newCgroupManager(_ cgroups.Config, _ int) Manager {
	return &NoopManager{}
}

// No-op. The actual pruning operations are implemented in Cgroup V1/V2 managers.
func pruneOldCgroups(_ cgroups.Config, _ log.FieldLogger) {}
