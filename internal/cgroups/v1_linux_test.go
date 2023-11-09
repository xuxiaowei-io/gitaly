//go:build linux

package cgroups

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

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
