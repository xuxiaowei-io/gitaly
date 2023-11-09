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

func defaultCgroupsConfig() cgroups.Config {
	return cgroups.Config{
		HierarchyRoot: "gitaly",
		Repositories: cgroups.Repositories{
			Count:       3,
			MemoryBytes: 1024000,
			CPUShares:   256,
			CPUQuotaUs:  200,
		},
	}
}

func TestSetup_RepoCgroups(t *testing.T) {
	tests := []struct {
		name            string
		cfg             cgroups.Repositories
		wantMemoryBytes int
		wantCPUShares   int
		wantCPUQuotaUs  int
		wantCFSPeriod   int
	}{
		{
			name:            "all config specified",
			cfg:             defaultCgroupsConfig().Repositories,
			wantMemoryBytes: 1024000,
			wantCPUShares:   256,
			wantCPUQuotaUs:  200,
			wantCFSPeriod:   int(cfsPeriodUs),
		},
		{
			name: "only memory limit set",
			cfg: cgroups.Repositories{
				MemoryBytes: 1024000,
			},
			wantMemoryBytes: 1024000,
		},
		{
			name: "only cpu shares set",
			cfg: cgroups.Repositories{
				CPUShares: 512,
			},
			wantCPUShares: 512,
		},
		{
			name: "only cpu quota set",
			cfg: cgroups.Repositories{
				CPUQuotaUs: 100,
			},
			wantCPUQuotaUs: 100,
			wantCFSPeriod:  int(cfsPeriodUs),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := newMockV1(t)

			pid := 1
			cfg := defaultCgroupsConfig()
			cfg.Repositories = tt.cfg
			cfg.Repositories.Count = 3
			cfg.HierarchyRoot = "gitaly"
			cfg.Mountpoint = mock.root

			v1Manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)

			require.False(t, v1Manager.Ready())
			require.NoError(t, v1Manager.Setup())
			require.True(t, v1Manager.Ready())

			ctx := testhelper.Context(t)

			// Validate no shards have been created. We deliberately do not call
			// `setupMockCgroupFiles()` here to confirm that the cgroup controller
			// is creating repository directories in the correct location.
			requireShardsV1(t, mock, v1Manager, pid)

			// Create a command to force Gitaly to create the repo cgroup.
			cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, cmd.Run())
			_, err := v1Manager.AddCommand(cmd)
			require.NoError(t, err)

			groupID := calcGroupID(cmd.Args, cfg.Repositories.Count)
			requireShardsV1(t, mock, v1Manager, pid, groupID)

			for i := 0; i < 3; i++ {
				cgroupExists := uint(i) == groupID

				memoryLimitPath := filepath.Join(
					mock.root, "memory", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "memory.limit_in_bytes",
				)
				if cgroupExists {
					requireCgroup(t, memoryLimitPath, tt.wantMemoryBytes)
				} else {
					require.NoFileExists(t, memoryLimitPath)
				}

				cpuSharesPath := filepath.Join(
					mock.root, "cpu", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.shares",
				)
				if cgroupExists {
					requireCgroup(t, cpuSharesPath, tt.wantCPUShares)
				} else {
					require.NoFileExists(t, cpuSharesPath)
				}

				cpuCFSQuotaPath := filepath.Join(
					mock.root, "cpu", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.cfs_quota_us",
				)
				if cgroupExists {
					requireCgroup(t, cpuCFSQuotaPath, tt.wantCPUQuotaUs)
				} else {
					require.NoFileExists(t, cpuCFSQuotaPath)
				}

				cpuCFSPeriodPath := filepath.Join(
					mock.root, "cpu", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.cfs_period_us",
				)
				if cgroupExists {
					requireCgroup(t, cpuCFSPeriodPath, tt.wantCFSPeriod)
				} else {
					require.NoFileExists(t, cpuCFSPeriodPath)
				}
			}
		})
	}
}

func TestAddCommand(t *testing.T) {
	mock := newMockV1(t)

	config := defaultCgroupsConfig()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16
	config.HierarchyRoot = "gitaly"
	config.Mountpoint = mock.root

	pid := 1
	groupID := calcGroupID(cmdArgs, config.Repositories.Count)

	v1Manager1 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)
	require.NoError(t, v1Manager1.Setup())
	ctx := testhelper.Context(t)

	cmd2 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	require.NoError(t, cmd2.Run())

	v1Manager2 := mock.newCgroupManager(config, testhelper.SharedLogger(t), pid)

	t.Run("without overridden key", func(t *testing.T) {
		_, err := v1Manager2.AddCommand(cmd2)
		require.NoError(t, err)
		requireShardsV1(t, mock, v1Manager2, pid, groupID)

		for _, s := range mock.subsystems {
			path := filepath.Join(mock.root, string(s.Name()), "gitaly",
				fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", groupID), "cgroup.procs")
			content := readCgroupFile(t, path)

			cmdPid, err := strconv.Atoi(string(content))
			require.NoError(t, err)

			require.Equal(t, cmd2.Process.Pid, cmdPid)
		}
	})

	t.Run("with overridden key", func(t *testing.T) {
		overridenGroupID := calcGroupID([]string{"foobar"}, config.Repositories.Count)

		_, err := v1Manager2.AddCommand(cmd2, WithCgroupKey("foobar"))
		require.NoError(t, err)

		for _, s := range mock.subsystems {
			path := filepath.Join(mock.root, string(s.Name()), "gitaly",
				fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", overridenGroupID), "cgroup.procs")
			content := readCgroupFile(t, path)

			cmdPid, err := strconv.Atoi(string(content))
			require.NoError(t, err)

			require.Equal(t, cmd2.Process.Pid, cmdPid)
		}
	})
}

func TestCleanup(t *testing.T) {
	mock := newMockV1(t)

	pid := 1
	cfg := defaultCgroupsConfig()
	cfg.Mountpoint = mock.root

	v1Manager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)

	require.NoError(t, v1Manager.Setup())
	require.NoError(t, v1Manager.Cleanup())

	for i := 0; i < 3; i++ {
		memoryPath := filepath.Join(mock.root, "memory", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i))
		cpuPath := filepath.Join(mock.root, "cpu", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i))

		require.NoDirExists(t, memoryPath)
		require.NoDirExists(t, cpuPath)
	}
}

func TestMetrics(t *testing.T) {
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
			expect: `# HELP gitaly_cgroup_cpu_usage_total CPU Usage of Cgroup
# TYPE gitaly_cgroup_cpu_usage_total gauge
gitaly_cgroup_cpu_usage_total{path="%s",type="kernel"} 0
gitaly_cgroup_cpu_usage_total{path="%s",type="user"} 0
# HELP gitaly_cgroup_memory_reclaim_attempts_total Number of memory usage hits limits
# TYPE gitaly_cgroup_memory_reclaim_attempts_total gauge
gitaly_cgroup_memory_reclaim_attempts_total{path="%s"} 2
# HELP gitaly_cgroup_procs_total Total number of procs
# TYPE gitaly_cgroup_procs_total gauge
gitaly_cgroup_procs_total{path="%s",subsystem="cpu"} 1
gitaly_cgroup_procs_total{path="%s",subsystem="memory"} 1
# HELP gitaly_cgroup_cpu_cfs_periods_total Number of elapsed enforcement period intervals
# TYPE gitaly_cgroup_cpu_cfs_periods_total counter
gitaly_cgroup_cpu_cfs_periods_total{path="%s"} 10
# HELP gitaly_cgroup_cpu_cfs_throttled_periods_total Number of throttled period intervals
# TYPE gitaly_cgroup_cpu_cfs_throttled_periods_total counter
gitaly_cgroup_cpu_cfs_throttled_periods_total{path="%s"} 20
# HELP gitaly_cgroup_cpu_cfs_throttled_seconds_total Total time duration the Cgroup has been throttled
# TYPE gitaly_cgroup_cpu_cfs_throttled_seconds_total counter
gitaly_cgroup_cpu_cfs_throttled_seconds_total{path="%s"} 0.001
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
			mock := newMockV1(t)

			config := defaultCgroupsConfig()
			config.Repositories.Count = 1
			config.Repositories.MemoryBytes = 1048576
			config.Repositories.CPUShares = 16
			config.Mountpoint = mock.root
			config.MetricsEnabled = tt.metricsEnabled

			v1Manager1 := mock.newCgroupManager(config, testhelper.SharedLogger(t), tt.pid)

			groupID := calcGroupID(cmdArgs, config.Repositories.Count)

			mock.setupMockCgroupFiles(t, v1Manager1, []uint{groupID}, mockCgroupFile{"memory.failcnt", "2"})
			require.NoError(t, v1Manager1.Setup())

			ctx := testhelper.Context(t)

			cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, cmd.Start())
			_, err := v1Manager1.AddCommand(cmd)
			require.NoError(t, err)

			gitCmd1 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, gitCmd1.Start())
			_, err = v1Manager1.AddCommand(gitCmd1)
			require.NoError(t, err)

			gitCmd2 := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
			require.NoError(t, gitCmd2.Start())
			_, err = v1Manager1.AddCommand(gitCmd2)
			require.NoError(t, err)

			requireShardsV1(t, mock, v1Manager1, tt.pid, groupID)

			defer func() {
				require.NoError(t, gitCmd2.Wait())
			}()

			require.NoError(t, cmd.Wait())
			require.NoError(t, gitCmd1.Wait())

			repoCgroupPath := filepath.Join(v1Manager1.currentProcessCgroup(), "repos-0")

			expected := strings.NewReader(strings.ReplaceAll(tt.expect, "%s", repoCgroupPath))
			assert.NoError(t, testutil.CollectAndCompare(v1Manager1, expected))
		})
	}
}

func TestPruneOldCgroups(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc           string
		cfg            cgroups.Config
		expectedPruned bool
		// setup returns a pid
		setup func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int
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
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int {
				pid := 1
				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
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
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int {
				pid := 1
				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
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
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int {
				cmd := exec.Command("ls")
				require.NoError(t, cmd.Run())
				pid := cmd.Process.Pid

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
				require.NoError(t, cgroupManager.Setup())

				memoryRoot := filepath.Join(
					cfg.Mountpoint,
					"memory",
					cfg.HierarchyRoot,
					"memory.limit_in_bytes",
				)
				require.NoError(t, os.WriteFile(memoryRoot, []byte{}, fs.ModeAppend))

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
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int {
				pid := os.Getpid()

				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), pid)
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
			setup: func(t *testing.T, cfg cgroups.Config, mock *mockCgroupV1) int {
				cgroupManager := mock.newCgroupManager(cfg, testhelper.SharedLogger(t), 0)
				require.NoError(t, cgroupManager.Setup())

				return 0
			},
			expectedPruned: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mock := newMockV1(t)
			tc.cfg.Mountpoint = mock.root

			memoryRoot := filepath.Join(
				tc.cfg.Mountpoint,
				"memory",
				tc.cfg.HierarchyRoot,
			)
			cpuRoot := filepath.Join(
				tc.cfg.Mountpoint,
				"cpu",
				tc.cfg.HierarchyRoot,
			)

			require.NoError(t, os.MkdirAll(cpuRoot, perm.PublicDir))
			require.NoError(t, os.MkdirAll(memoryRoot, perm.PublicDir))

			pid := tc.setup(t, tc.cfg, mock)

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			mock.pruneOldCgroups(tc.cfg, logger)

			// create cgroups directories with a different pid
			oldGitalyProcessMemoryDir := filepath.Join(
				memoryRoot,
				fmt.Sprintf("gitaly-%d", pid),
			)
			oldGitalyProcesssCPUDir := filepath.Join(
				cpuRoot,
				fmt.Sprintf("gitaly-%d", pid),
			)

			if tc.expectedPruned {
				require.NoDirExists(t, oldGitalyProcessMemoryDir)
				require.NoDirExists(t, oldGitalyProcesssCPUDir)
			} else {
				require.DirExists(t, oldGitalyProcessMemoryDir)
				require.DirExists(t, oldGitalyProcesssCPUDir)
				require.Empty(t, hook.AllEntries())
			}
		})
	}
}

func TestStatsV1(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		mockFiles     []mockCgroupFile
		expectedStats Stats
	}{
		{
			desc: "empty statistics",
			mockFiles: []mockCgroupFile{
				{"memory.limit_in_bytes", "0"},
				{"memory.usage_in_bytes", "0"},
				{"memory.oom_control", ""},
				{"cpu.stat", ""},
			},
			expectedStats: Stats{},
		},
		{
			desc: "cgroupfs recorded some stats",
			mockFiles: []mockCgroupFile{
				{"memory.limit_in_bytes", "2000000000"},
				{"memory.usage_in_bytes", "1234000000"},
				{"memory.oom_control", `oom_kill_disable 1
under_oom 1
oom_kill 3`},
				{"cpu.stat", `nr_periods 10
nr_throttled 50
throttled_time 1000000`}, // 0.001 seconds
				{"memory.stat", `cache 235000000
rss 234000000
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
					OOMKills:             3,
					UnderOOM:             true,
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
			mock := newMockV1(t)

			config := defaultCgroupsConfig()
			config.Repositories.Count = 1
			config.Repositories.MemoryBytes = 2000000000
			config.Repositories.CPUShares = 16
			config.Mountpoint = mock.root

			v1Manager := mock.newCgroupManager(config, testhelper.SharedLogger(t), 1)

			mock.setupMockCgroupFiles(t, v1Manager, []uint{}, tc.mockFiles...)
			require.NoError(t, v1Manager.Setup())

			stats, err := v1Manager.Stats()
			require.NoError(t, err)
			require.Equal(t, tc.expectedStats, stats)
		})
	}
}

func requireShardsV1(t *testing.T, mock *mockCgroupV1, mgr *CGroupManager, pid int, expectedShards ...uint) {
	t.Helper()

	for shard := uint(0); shard < mgr.cfg.Repositories.Count; shard++ {
		for _, s := range mock.subsystems {
			cgroupPath := filepath.Join("gitaly",
				fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", shard))
			diskPath := filepath.Join(mock.root, string(s.Name()), cgroupPath)

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
}

func requireCgroup(t *testing.T, cgroupFile string, want int) {
	t.Helper()

	if want <= 0 {
		// If files doesn't exist kernel will create it with default values
		require.NoFileExistsf(t, cgroupFile, "cgroup file should not exist: %q", cgroupFile)
		return
	}

	require.Equal(t,
		string(readCgroupFile(t, cgroupFile)),
		strconv.Itoa(want),
	)
}

func readCgroupFile(t *testing.T, path string) []byte {
	t.Helper()

	// The cgroups package defaults to permission 0 as it expects the file to be existing (the kernel creates the file)
	// and its testing override the permission private variable to something sensible, hence we have to chmod ourselves
	// so we can read the file.
	require.NoError(t, os.Chmod(path, perm.PublicFile))

	return testhelper.MustReadFile(t, path)
}
