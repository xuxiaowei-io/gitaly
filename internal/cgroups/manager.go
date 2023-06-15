//go:build !linux

package cgroups

import (
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
)

func newCgroupManager(cfg cgroupscfg.Config, pid int) Manager {
	return &NoopManager{}
}
