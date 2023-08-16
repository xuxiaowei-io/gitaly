//go:build !linux

package cgroups

import (
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

func newCgroupManager(cgroups.Config, logrus.FieldLogger, int) Manager {
	return &NoopManager{}
}

// No-op. The actual pruning operations are implemented in Cgroup V1/V2 managers.
func pruneOldCgroups(cgroups.Config, logrus.FieldLogger) {}
