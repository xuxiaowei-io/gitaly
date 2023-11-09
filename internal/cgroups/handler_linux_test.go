//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strconv"
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

type expectedCgroup struct {
	wantMemoryBytes int
	wantCPUShares   int
	wantCPUQuotaUs  int
	wantCFSPeriod   int
	wantCPUWeight   int
	wantCPUMax      string
}

func TestSetup_ParentCgroups(t *testing.T) {
	tests := []struct {
		name       string
		cfg        cgroups.Config
		expectedV1 expectedCgroup
		expectedV2 expectedCgroup
	}{
		{
			name: "all config specified",
			cfg: cgroups.Config{
				MemoryBytes: 102400,
				CPUShares:   256,
				CPUQuotaUs:  200,
			},
			expectedV1: expectedCgroup{
				wantMemoryBytes: 102400,
				wantCPUShares:   256,
				wantCPUQuotaUs:  200,
				wantCFSPeriod:   int(cfsPeriodUs),
			},
			expectedV2: expectedCgroup{
				wantMemoryBytes: 102400,
				wantCPUWeight:   256,
				wantCPUMax:      "200 100000",
			},
		},
		{
			name: "only memory limit set",
			cfg: cgroups.Config{
				MemoryBytes: 102400,
			},
			expectedV1: expectedCgroup{
				wantMemoryBytes: 102400,
			},
			expectedV2: expectedCgroup{
				wantMemoryBytes: 102400,
			},
		},
		{
			name: "only cpu shares set",
			cfg: cgroups.Config{
				CPUShares: 512,
			},
			expectedV1: expectedCgroup{
				wantCPUShares: 512,
			},
			expectedV2: expectedCgroup{
				wantCPUWeight: 512,
			},
		},
		{
			name: "only cpu quota set",
			cfg: cgroups.Config{
				CPUQuotaUs: 200,
			},
			expectedV1: expectedCgroup{
				wantCPUQuotaUs: 200,
				wantCFSPeriod:  int(cfsPeriodUs),
			},
			expectedV2: expectedCgroup{
				wantCPUMax: "200 100000",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for _, version := range []int{1, 2} {
				version := version
				t.Run("cgroups-v"+strconv.Itoa(version), func(t *testing.T) {
					t.Parallel()

					mock := newMock(t, version)

					pid := 1

					cfg := tt.cfg
					cfg.HierarchyRoot = "gitaly"
					cfg.Mountpoint = mock.rootPath()

					manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
					mock.setupMockCgroupFiles(t, manager, []uint{})

					require.False(t, manager.Ready())
					require.NoError(t, manager.Setup())
					require.True(t, manager.Ready())

					cgroupPath := filepath.Join("gitaly", fmt.Sprintf("gitaly-%d", pid))
					if version == 1 {
						requireCgroupComponents(t, version, mock.rootPath(), cgroupPath, tt.expectedV1)
					} else {
						requireCgroupComponents(t, version, mock.rootPath(), cgroupPath, tt.expectedV2)
					}
				})
			}
		})
	}
}

func requireCgroupComponents(t *testing.T, version int, root string, cgroupPath string, expected expectedCgroup) {
	t.Helper()

	if version == 1 {
		memoryLimitPath := filepath.Join(root, "memory", cgroupPath, "memory.limit_in_bytes")
		requireCgroupWithInt(t, memoryLimitPath, expected.wantMemoryBytes)

		cpuSharesPath := filepath.Join(root, "cpu", cgroupPath, "cpu.shares")
		requireCgroupWithInt(t, cpuSharesPath, expected.wantCPUShares)

		cpuCFSQuotaPath := filepath.Join(root, "cpu", cgroupPath, "cpu.cfs_quota_us")
		requireCgroupWithInt(t, cpuCFSQuotaPath, expected.wantCPUQuotaUs)

		cpuCFSPeriodPath := filepath.Join(root, "cpu", cgroupPath, "cpu.cfs_period_us")
		requireCgroupWithInt(t, cpuCFSPeriodPath, expected.wantCFSPeriod)
	} else {
		memoryMaxPath := filepath.Join(root, cgroupPath, "memory.max")
		requireCgroupWithInt(t, memoryMaxPath, expected.wantMemoryBytes)

		cpuWeightPath := filepath.Join(root, cgroupPath, "cpu.weight")
		requireCgroupWithInt(t, cpuWeightPath, calculateWantCPUWeight(expected.wantCPUWeight))

		cpuMaxPath := filepath.Join(root, cgroupPath, "cpu.max")
		requireCgroupWithString(t, cpuMaxPath, expected.wantCPUMax)
	}
}
