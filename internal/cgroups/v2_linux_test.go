//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
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
