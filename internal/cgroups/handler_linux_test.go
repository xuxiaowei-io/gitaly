//go:build linux

package cgroups

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	cgrps "github.com/containerd/cgroups/v3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"golang.org/x/exp/slices"
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

func TestRepoCgroups(t *testing.T) {
	tests := []struct {
		name       string
		cfg        cgroups.Repositories
		expectedV1 expectedCgroup
		expectedV2 expectedCgroup
	}{
		{
			name: "all config specified",
			cfg:  defaultCgroupsConfig().Repositories,
			expectedV1: expectedCgroup{
				wantMemoryBytes: 1024000,
				wantCPUShares:   256,
				wantCPUQuotaUs:  200,
				wantCFSPeriod:   int(cfsPeriodUs),
			},
			expectedV2: expectedCgroup{
				wantMemoryBytes: 1024000,
				wantCPUWeight:   256,
				wantCPUMax:      "200 100000",
			},
		},
		{
			name: "only memory limit set",
			cfg: cgroups.Repositories{
				MemoryBytes: 1024000,
			},
			expectedV1: expectedCgroup{
				wantMemoryBytes: 1024000,
			},
			expectedV2: expectedCgroup{
				wantMemoryBytes: 1024000,
			},
		},
		{
			name: "only cpu shares set",
			cfg: cgroups.Repositories{
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
			cfg: cgroups.Repositories{
				CPUQuotaUs: 100,
			},
			expectedV1: expectedCgroup{
				wantCPUQuotaUs: 100,
				wantCFSPeriod:  int(cfsPeriodUs),
			},
			expectedV2: expectedCgroup{
				wantCPUMax: "100 100000",
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
					cfg := defaultCgroupsConfig()
					cfg.Repositories = tt.cfg
					cfg.Repositories.Count = 3
					cfg.HierarchyRoot = "gitaly"
					cfg.Mountpoint = mock.rootPath()

					manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)

					// Validate no shards have been created. We deliberately do not call
					// `setupMockCgroupFiles()` here to confirm that the cgroup controller
					// is creating repository directories in the correct location.
					requireShards(t, version, mock, manager, pid)

					groupID := calcGroupID(cmdArgs, cfg.Repositories.Count)

					mock.setupMockCgroupFiles(t, manager, []uint{groupID})

					require.False(t, manager.Ready())
					require.NoError(t, manager.Setup())
					require.True(t, manager.Ready())

					ctx := testhelper.Context(t)

					// Create a command to force Gitaly to create the repo cgroup.
					cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
					require.NoError(t, cmd.Run())
					_, err := manager.AddCommand(cmd)
					require.NoError(t, err)

					requireShards(t, version, mock, manager, pid, groupID)

					var expected expectedCgroup
					if version == 1 {
						expected = tt.expectedV1
					} else {
						expected = tt.expectedV2
					}

					for shard := uint(0); shard < cfg.Repositories.Count; shard++ {
						// The negative case where no directory should exist is asserted
						// by `requireShards()`.
						if shard == groupID {
							cgRelPath := filepath.Join(
								"gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", shard),
							)

							requireCgroupComponents(t, version, mock.rootPath(), cgRelPath, expected)
						}
					}
				})
			}
		})
	}
}

func TestAddCommand(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run("cgroups-v"+strconv.Itoa(version), func(t *testing.T) {
			mock := newMock(t, version)

			config := defaultCgroupsConfig()
			config.Repositories.Count = 10
			config.Repositories.MemoryBytes = 1024
			config.Repositories.CPUShares = 16
			config.HierarchyRoot = "gitaly"
			config.Mountpoint = mock.rootPath()

			pid := 1
			manager1 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)
			mock.setupMockCgroupFiles(t, manager1, []uint{})
			require.NoError(t, manager1.Setup())

			ctx := testhelper.Context(t)

			cmd2 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, cmd2.Run())

			groupID := calcGroupID(cmd2.Args, config.Repositories.Count)

			manager2 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)

			t.Run("without overridden key", func(t *testing.T) {
				_, err := manager2.AddCommand(cmd2)
				require.NoError(t, err)
				requireShards(t, version, mock, manager2, pid, groupID)

				for _, path := range mock.repoPaths(pid, groupID) {
					procsPath := filepath.Join(path, "cgroup.procs")
					content := readCgroupFile(t, procsPath)

					cmdPid, err := strconv.Atoi(string(content))
					require.NoError(t, err)

					require.Equal(t, cmd2.Process.Pid, cmdPid)
				}
			})

			t.Run("with overridden key", func(t *testing.T) {
				overriddenGroupID := calcGroupID([]string{"foobar"}, config.Repositories.Count)

				_, err := manager2.AddCommand(cmd2, WithCgroupKey("foobar"))
				require.NoError(t, err)
				requireShards(t, version, mock, manager2, pid, groupID, overriddenGroupID)

				for _, path := range mock.repoPaths(pid, overriddenGroupID) {
					procsPath := filepath.Join(path, "cgroup.procs")
					content := readCgroupFile(t, procsPath)

					cmdPid, err := strconv.Atoi(string(content))
					require.NoError(t, err)

					require.Equal(t, cmd2.Process.Pid, cmdPid)
				}
			})
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

func requireShards(t *testing.T, version int, mock mockCgroup, mgr *CGroupManager, pid int, expectedShards ...uint) {
	for shard := uint(0); shard < mgr.cfg.Repositories.Count; shard++ {
		shouldExist := slices.Contains(expectedShards, shard)

		cgroupPath := filepath.Join("gitaly",
			fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", shard))
		cgLock := mgr.status.getLock(cgroupPath)
		require.Equal(t, shouldExist, cgLock.isCreated())

		for _, diskPath := range mock.repoPaths(pid, shard) {
			if shouldExist {
				require.DirExists(t, diskPath)
			} else {
				require.NoDirExists(t, diskPath)
			}
		}
	}
}
