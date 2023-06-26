//go:build !linux

package cgroups

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

func newCgroupManager(cfg cgroupscfg.Config, pid int) Manager {
	return &NoopManager{}
}

func pruneOldCgroups(cfg cgroups.Config, logger log.FieldLogger) {
	return
}
