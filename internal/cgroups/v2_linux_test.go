//go:build linux

package cgroups

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
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
