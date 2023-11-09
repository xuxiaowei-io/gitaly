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
