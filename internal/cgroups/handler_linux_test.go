//go:build linux

package cgroups

import (
	"testing"

	cgrps "github.com/containerd/cgroups/v3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestNewManager(t *testing.T) {
	cfg := cgroups.Config{Repositories: cgroups.Repositories{Count: 10}}

	manager := newCgroupManagerWithMode(cfg, testhelper.SharedLogger(t), 1, cgrps.Legacy)
	require.IsType(t, &cgroupV1Handler{}, manager.handler)
	manager = newCgroupManagerWithMode(cfg, testhelper.SharedLogger(t), 1, cgrps.Hybrid)
	require.IsType(t, &cgroupV1Handler{}, manager.handler)
	manager = newCgroupManagerWithMode(cfg, testhelper.SharedLogger(t), 1, cgrps.Unified)
	require.IsType(t, &cgroupV2Handler{}, manager.handler)
	manager = newCgroupManagerWithMode(cfg, testhelper.SharedLogger(t), 1, cgrps.Unavailable)
	require.Nil(t, manager)
}
