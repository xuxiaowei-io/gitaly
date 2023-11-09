//go:build linux

package cgroups

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"golang.org/x/exp/slices"
)

func defaultCgroupsV2Config() cgroups.Config {
	return cgroups.Config{
		HierarchyRoot: "gitaly",
		Repositories: cgroups.Repositories{
			Count:       3,
			MemoryBytes: 1024000,
			CPUShares:   256,
			CPUQuotaUs:  2000,
		},
	}
}

func TestSetup_ParentCgroupsV2(t *testing.T) {
	tests := []struct {
		name            string
		cfg             cgroups.Config
		wantMemoryBytes int
		wantCPUWeight   int
		wantCPUMax      string
	}{
		{
			name: "all config specified",
			cfg: cgroups.Config{
				MemoryBytes: 102400,
				CPUShares:   256,
				CPUQuotaUs:  2000,
			},
			wantMemoryBytes: 102400,
			wantCPUWeight:   256,
			wantCPUMax:      "2000 100000",
		},
		{
			name: "only memory limit set",
			cfg: cgroups.Config{
				MemoryBytes: 102400,
			},
			wantMemoryBytes: 102400,
		},
		{
			name: "only cpu shares set",
			cfg: cgroups.Config{
				CPUShares: 512,
			},
			wantCPUWeight: 512,
		},
		{
			name: "only cpu quota set",
			cfg: cgroups.Config{
				CPUQuotaUs: 2000,
			},
			wantCPUMax: "2000 100000",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := newMockV2(t)

			pid := 1
			tt.cfg.HierarchyRoot = "gitaly"
			tt.cfg.Mountpoint = mock.root

			v2Manager := mock.newCgroupManager(tt.cfg, testhelper.SharedLogger(t), pid)
			mock.setupMockCgroupFiles(t, v2Manager, []uint{})

			require.False(t, v2Manager.Ready())
			require.NoError(t, v2Manager.Setup())
			require.True(t, v2Manager.Ready())

			memoryMaxPath := filepath.Join(
				mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), "memory.max",
			)
			requireCgroupWithInt(t, memoryMaxPath, tt.wantMemoryBytes)

			cpuWeightPath := filepath.Join(
				mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), "cpu.weight",
			)
			requireCgroupWithInt(t, cpuWeightPath, calculateWantCPUWeight(tt.wantCPUWeight))

			cpuMaxPath := filepath.Join(
				mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), "cpu.max",
			)
			requireCgroupWithString(t, cpuMaxPath, tt.wantCPUMax)
		})
	}
}

func TestSetup_RepoCgroupsV2(t *testing.T) {
	tests := []struct {
		name            string
		cfg             cgroups.Repositories
		wantMemoryBytes int
		wantCPUWeight   int
		wantCPUMax      string
	}{
		{
			name:            "all config specified",
			cfg:             defaultCgroupsV2Config().Repositories,
			wantMemoryBytes: 1024000,
			wantCPUWeight:   256,
			wantCPUMax:      "2000 100000",
		},
		{
			name: "only memory limit set",
			cfg: cgroups.Repositories{
				Count:       3,
				MemoryBytes: 1024000,
			},
			wantMemoryBytes: 1024000,
		},
		{
			name: "only cpu shares set",
			cfg: cgroups.Repositories{
				Count:     3,
				CPUShares: 512,
			},
			wantCPUWeight: 512,
		},
		{
			name: "only cpu quota set",
			cfg: cgroups.Repositories{
				Count:      3,
				CPUQuotaUs: 1000,
			},
			wantCPUMax: "1000 100000",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := newMockV2(t)

			pid := 1

			cfg := defaultCgroupsV2Config()
			cfg.Mountpoint = mock.root
			cfg.Repositories = tt.cfg

			groupID := calcGroupID(cmdArgs, cfg.Repositories.Count)

			v2Manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)

			// Validate no shards have been created. We deliberately do not call
			// `setupMockCgroupFiles()` here to confirm that the cgroup controller
			// is creating repository directories in the correct location.
			requireShardsV2(t, mock, v2Manager, pid)

			mock.setupMockCgroupFiles(t, v2Manager, []uint{groupID})

			require.False(t, v2Manager.Ready())
			require.NoError(t, v2Manager.Setup())
			require.True(t, v2Manager.Ready())

			ctx := testhelper.Context(t)

			// Create a command to force Gitaly to create the repo cgroup.
			cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, cmd.Run())
			_, err := v2Manager.AddCommand(cmd)
			require.NoError(t, err)

			requireShardsV2(t, mock, v2Manager, pid, groupID)

			for i := 0; i < 3; i++ {
				cgroupExists := uint(i) == groupID

				memoryMaxPath := filepath.Join(
					mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "memory.max",
				)

				if cgroupExists {
					requireCgroupWithInt(t, memoryMaxPath, tt.wantMemoryBytes)
				} else {
					require.NoFileExists(t, memoryMaxPath)
				}

				cpuWeightPath := filepath.Join(
					mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.weight",
				)

				if cgroupExists {
					requireCgroupWithInt(t, cpuWeightPath, calculateWantCPUWeight(tt.wantCPUWeight))
				} else {
					require.NoFileExists(t, cpuWeightPath)
				}

				cpuMaxPath := filepath.Join(
					mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.max",
				)

				if cgroupExists {
					requireCgroupWithString(t, cpuMaxPath, tt.wantCPUMax)
				} else {
					require.NoFileExists(t, cpuMaxPath)
				}
			}
		})
	}
}

func TestAddCommandV2(t *testing.T) {
	mock := newMockV2(t)

	config := defaultCgroupsV2Config()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16
	config.Mountpoint = mock.root

	pid := 1
	groupID := calcGroupID(cmdArgs, config.Repositories.Count)

	v2Manager1 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)
	mock.setupMockCgroupFiles(t, v2Manager1, []uint{})

	require.NoError(t, v2Manager1.Setup())
	ctx := testhelper.Context(t)

	cmd2 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	require.NoError(t, cmd2.Run())

	v2Manager2 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)

	t.Run("without overridden key", func(t *testing.T) {
		groupID := calcGroupID(cmd2.Args, config.Repositories.Count)

		_, err := v2Manager2.AddCommand(cmd2)
		require.NoError(t, err)
		requireShardsV2(t, mock, v2Manager2, pid, groupID)

		path := filepath.Join(mock.root, "gitaly",
			fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", groupID), "cgroup.procs")
		content := readCgroupFile(t, path)

		cmdPid, err := strconv.Atoi(string(content))
		require.NoError(t, err)

		require.Equal(t, cmd2.Process.Pid, cmdPid)
	})

	t.Run("with overridden key", func(t *testing.T) {
		overriddenGroupID := calcGroupID([]string{"foobar"}, config.Repositories.Count)

		_, err := v2Manager2.AddCommand(cmd2, WithCgroupKey("foobar"))
		require.NoError(t, err)
		requireShardsV2(t, mock, v2Manager2, pid, groupID, overriddenGroupID)

		path := filepath.Join(mock.root, "gitaly",
			fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", overriddenGroupID), "cgroup.procs")
		content := readCgroupFile(t, path)

		cmdPid, err := strconv.Atoi(string(content))
		require.NoError(t, err)

		require.Equal(t, cmd2.Process.Pid, cmdPid)
	})
}

func TestCleanupV2(t *testing.T) {
	mock := newMockV2(t)

	pid := 1
	cfg := defaultCgroupsV2Config()
	cfg.Mountpoint = mock.root

	v2Manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
	mock.setupMockCgroupFiles(t, v2Manager, []uint{0, 1, 2})

	require.NoError(t, v2Manager.Setup())
	require.NoError(t, v2Manager.Cleanup())

	for i := 0; i < 3; i++ {
		require.NoDirExists(t, filepath.Join(mock.root, "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i)))
	}
}

func TestMetricsV2(t *testing.T) {
	tests := []struct {
		name           string
		metricsEnabled bool
		pid            int
		expect         string
	}{
		{
			name:           "metrics enabled: true",
			metricsEnabled: true,
			pid:            1,
			expect: `# HELP gitaly_cgroup_cpu_cfs_periods_total Number of elapsed enforcement period intervals
# TYPE gitaly_cgroup_cpu_cfs_periods_total counter
gitaly_cgroup_cpu_cfs_periods_total{path="%s"} 10
# HELP gitaly_cgroup_cpu_cfs_throttled_periods_total Number of throttled period intervals
# TYPE gitaly_cgroup_cpu_cfs_throttled_periods_total counter
gitaly_cgroup_cpu_cfs_throttled_periods_total{path="%s"} 20
# HELP gitaly_cgroup_cpu_cfs_throttled_seconds_total Total time duration the Cgroup has been throttled
# TYPE gitaly_cgroup_cpu_cfs_throttled_seconds_total counter
gitaly_cgroup_cpu_cfs_throttled_seconds_total{path="%s"} 0.001
# HELP gitaly_cgroup_cpu_usage_total CPU Usage of Cgroup
# TYPE gitaly_cgroup_cpu_usage_total gauge
gitaly_cgroup_cpu_usage_total{path="%s",type="kernel"} 0
gitaly_cgroup_cpu_usage_total{path="%s",type="user"} 0
# HELP gitaly_cgroup_procs_total Total number of procs
# TYPE gitaly_cgroup_procs_total gauge
gitaly_cgroup_procs_total{path="%s",subsystem="cpu"} 1
gitaly_cgroup_procs_total{path="%s",subsystem="cpuset"} 1
gitaly_cgroup_procs_total{path="%s",subsystem="memory"} 1
`,
		},
		{
			name:           "metrics enabled: false",
			metricsEnabled: false,
			pid:            2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock := newMockV2(t)

			config := defaultCgroupsV2Config()
			config.Repositories.Count = 1
			config.Repositories.MemoryBytes = 1048576
			config.Repositories.CPUShares = 16
			config.Mountpoint = mock.root
			config.MetricsEnabled = tt.metricsEnabled

			groupID := calcGroupID(cmdArgs, config.Repositories.Count)
			v2Manager1 := mock.newCgroupManager(config, testhelper.SharedLogger(t), tt.pid)

			mock.setupMockCgroupFiles(t, v2Manager1, []uint{groupID})
			require.NoError(t, v2Manager1.Setup())

			ctx := testhelper.Context(t)

			cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, cmd.Start())
			_, err := v2Manager1.AddCommand(cmd)
			require.NoError(t, err)

			gitCmd1 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, gitCmd1.Start())
			_, err = v2Manager1.AddCommand(gitCmd1)
			require.NoError(t, err)

			gitCmd2 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, gitCmd2.Start())
			_, err = v2Manager1.AddCommand(gitCmd2)
			require.NoError(t, err)

			requireShardsV2(t, mock, v2Manager1, tt.pid, groupID)

			defer func() {
				require.NoError(t, gitCmd2.Wait())
			}()

			require.NoError(t, cmd.Wait())
			require.NoError(t, gitCmd1.Wait())

			repoCgroupPath := filepath.Join(v2Manager1.currentProcessCgroup(), "repos-0")

			expected := strings.NewReader(strings.ReplaceAll(tt.expect, "%s", repoCgroupPath))

			assert.NoError(t, testutil.CollectAndCompare(v2Manager1, expected))
		})
	}
}

func TestPruneOldCgroupsV2(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc           string
		cfg            cgroups.Config
		expectedPruned bool
		// setup returns a pid
		setup func(*testing.T, cgroups.Config, *mockCgroupV2) int
	}{
		{
			desc: "process belongs to another user",
			cfg: cgroups.Config{
				HierarchyRoot: "gitaly",
				Repositories: cgroups.Repositories{
					Count:       10,
					MemoryBytes: 10 * 1024 * 1024,
					CPUShares:   1024,
				},
			},
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV2) int {
				pid := 1

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
				mock.setupMockCgroupFiles(t, cgroupManager, []uint{0, 1, 2})
				require.NoError(t, cgroupManager.Setup())

				return pid
			},
			expectedPruned: true,
		},
		{
			desc: "no hierarchy root",
			cfg: cgroups.Config{
				HierarchyRoot: "",
				Repositories: cgroups.Repositories{
					Count:       10,
					MemoryBytes: 10 * 1024 * 1024,
					CPUShares:   1024,
				},
			},
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV2) int {
				pid := 1

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
				mock.setupMockCgroupFiles(t, cgroupManager, []uint{0, 1, 2})
				require.NoError(t, cgroupManager.Setup())
				return 1
			},
			expectedPruned: false,
		},
		{
			desc: "pid of finished process",
			cfg: cgroups.Config{
				HierarchyRoot: "gitaly",
				Repositories: cgroups.Repositories{
					Count:       10,
					MemoryBytes: 10 * 1024 * 1024,
					CPUShares:   1024,
				},
			},
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV2) int {
				cmd := exec.Command("ls")
				require.NoError(t, cmd.Run())
				pid := cmd.Process.Pid

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
				mock.setupMockCgroupFiles(t, cgroupManager, []uint{0, 1, 2})
				require.NoError(t, cgroupManager.Setup())

				memoryFile := filepath.Join(
					cfg.Mountpoint,
					cfg.HierarchyRoot,
					"memory.limit_in_bytes",
				)
				require.NoError(t, os.WriteFile(memoryFile, []byte{}, fs.ModeAppend))

				return pid
			},
			expectedPruned: true,
		},
		{
			desc: "pid of running process",
			cfg: cgroups.Config{
				HierarchyRoot: "gitaly",
				Repositories: cgroups.Repositories{
					Count:       10,
					MemoryBytes: 10 * 1024 * 1024,
					CPUShares:   1024,
				},
			},
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV2) int {
				pid := os.Getpid()

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
				mock.setupMockCgroupFiles(t, cgroupManager, []uint{0, 1, 2})
				require.NoError(t, cgroupManager.Setup())

				return pid
			},
			expectedPruned: false,
		},
		{
			desc: "gitaly-0 directory is deleted",
			cfg: cgroups.Config{
				HierarchyRoot: "gitaly",
				Repositories: cgroups.Repositories{
					Count:       10,
					MemoryBytes: 10 * 1024 * 1024,
					CPUShares:   1024,
				},
			},
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV2) int {
				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), 0)
				mock.setupMockCgroupFiles(t, cgroupManager, []uint{0, 1, 2})
				require.NoError(t, cgroupManager.Setup())

				return 0
			},
			expectedPruned: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mock := newMockV2(t)
			tc.cfg.Mountpoint = mock.root

			root := filepath.Join(
				tc.cfg.Mountpoint,
				tc.cfg.HierarchyRoot,
			)
			require.NoError(t, os.MkdirAll(root, perm.PublicDir))

			pid := tc.setup(t, tc.cfg, mock)

			logger := testhelper.NewLogger(t)
			mock.pruneOldCgroups(tc.cfg, logger)

			// create cgroups directories with a different pid
			oldGitalyProcessDir := filepath.Join(
				root,
				fmt.Sprintf("gitaly-%d", pid),
			)

			if tc.expectedPruned {
				require.NoDirExists(t, oldGitalyProcessDir)
			} else {
				require.DirExists(t, oldGitalyProcessDir)
			}
		})
	}
}

func TestStatsV2(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		mockFiles     []mockCgroupFile
		expectedStats Stats
	}{
		{
			desc: "empty statistics",
			mockFiles: []mockCgroupFile{
				{"memory.current", "0"},
				{"memory.max", "0"},
				{"cpu.stat", ""},
			},
			expectedStats: Stats{},
		},
		{
			desc: "cgroupfs recorded some stats",
			mockFiles: []mockCgroupFile{
				{"memory.max", "2000000000"},
				{"memory.current", "1234000000"},
				{"memory.events", `low 1
high 2
max 3
oom 4
oom_kill 5`},
				{"nr_throttled", "50"},
				{"throttled_usec", "1000000"},
				{"cpu.stat", `nr_periods 10
nr_throttled 50
throttled_usec 1000000`}, // 0.001 seconds
				{"memory.stat", `anon 234000000
file 235000000
inactive_anon 200000000
active_anon 34000000
inactive_file 100000000
active_file 135000000`},
			},
			expectedStats: Stats{
				ParentStats: CgroupStats{
					CPUThrottledCount:    50,
					CPUThrottledDuration: 0.001,
					MemoryUsage:          1234000000,
					MemoryLimit:          2000000000,
					OOMKills:             5,
					Anon:                 234000000,
					ActiveAnon:           34000000,
					InactiveAnon:         200000000,
					File:                 235000000,
					ActiveFile:           135000000,
					InactiveFile:         100000000,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := newMockV2(t)

			config := defaultCgroupsConfig()
			config.Repositories.Count = 1
			config.Repositories.MemoryBytes = 2000000000
			config.Repositories.CPUShares = 16
			config.Mountpoint = mock.root

			v2Manager := mock.newCgroupManager(config, testhelper.SharedLogger(t), 1)

			mock.setupMockCgroupFiles(t, v2Manager, []uint{0}, tc.mockFiles...)
			require.NoError(t, v2Manager.Setup())

			stats, err := v2Manager.Stats()
			require.NoError(t, err)
			require.Equal(t, tc.expectedStats, stats)
		})
	}
}

func calculateWantCPUWeight(wantCPUWeight int) int {
	if wantCPUWeight == 0 {
		return 0
	}
	return 1 + ((wantCPUWeight-2)*9999)/262142
}

func requireShardsV2(t *testing.T, mock *mockCgroupV2, mgr *CGroupManager, pid int, expectedShards ...uint) {
	t.Helper()

	for shard := uint(0); shard < mgr.cfg.Repositories.Count; shard++ {
		cgroupPath := filepath.Join("gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", shard))
		diskPath := filepath.Join(mock.root, cgroupPath)

		if slices.Contains(expectedShards, shard) {
			require.DirExists(t, diskPath)

			cgLock := mgr.status.getLock(cgroupPath)
			require.True(t, cgLock.isCreated())
		} else {
			require.NoDirExists(t, diskPath)

			// Confirm we pre-populated this map entry.
			_, lockInserted := mgr.status.m[cgroupPath]
			require.True(t, lockInserted)
		}
	}
}

func requireCgroupWithString(t *testing.T, cgroupFile string, want string) {
	t.Helper()

	if want == "" {
		return
	}
	require.Equal(t,
		string(readCgroupFile(t, cgroupFile)),
		want,
	)
}

func requireCgroupWithInt(t *testing.T, cgroupFile string, want int) {
	t.Helper()

	if want <= 0 {
		return
	}

	require.Equal(t,
		string(readCgroupFile(t, cgroupFile)),
		strconv.Itoa(want),
	)
}
