package cgroups

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func defaultCgroupsConfig() cgroups.Config {
	return cgroups.Config{
		HierarchyRoot: "gitaly",
		Repositories: cgroups.Repositories{
			Count:       3,
			MemoryBytes: 1024000,
			CPUShares:   256,
		},
	}
}

func TestSetup(t *testing.T) {
	mock := newMock(t)

	pid := 1
	v1Manager := &CGroupV1Manager{
		cfg:       defaultCgroupsConfig(),
		hierarchy: mock.hierarchy,
		pid:       pid,
	}
	require.NoError(t, v1Manager.Setup())

	for i := 0; i < 3; i++ {
		memoryPath := filepath.Join(
			mock.root, "memory", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "memory.limit_in_bytes",
		)
		memoryContent := readCgroupFile(t, memoryPath)

		require.Equal(t, string(memoryContent), "1024000")

		cpuPath := filepath.Join(
			mock.root, "cpu", "gitaly", fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", i), "cpu.shares",
		)
		cpuContent := readCgroupFile(t, cpuPath)

		require.Equal(t, string(cpuContent), "256")
	}
}

func TestAddCommand(t *testing.T) {
	mock := newMock(t)

	config := defaultCgroupsConfig()
	config.Repositories.Count = 10
	config.Repositories.MemoryBytes = 1024
	config.Repositories.CPUShares = 16

	pid := 1
	v1Manager1 := &CGroupV1Manager{
		cfg:       config,
		hierarchy: mock.hierarchy,
		pid:       pid,
	}
	require.NoError(t, v1Manager1.Setup())
	ctx := testhelper.Context(t)

	cmd2 := exec.CommandContext(ctx, "ls", "-hal", ".")
	require.NoError(t, cmd2.Run())

	v1Manager2 := &CGroupV1Manager{
		cfg:       config,
		hierarchy: mock.hierarchy,
		pid:       pid,
	}

	t.Run("without overridden key", func(t *testing.T) {
		_, err := v1Manager2.AddCommand(cmd2)
		require.NoError(t, err)

		checksum := crc32.ChecksumIEEE([]byte(strings.Join(cmd2.Args, "/")))
		groupID := uint(checksum) % config.Repositories.Count

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
		_, err := v1Manager2.AddCommand(cmd2, WithCgroupKey("foobar"))
		require.NoError(t, err)

		checksum := crc32.ChecksumIEEE([]byte("foobar"))
		groupID := uint(checksum) % config.Repositories.Count

		for _, s := range mock.subsystems {
			path := filepath.Join(mock.root, string(s.Name()), "gitaly",
				fmt.Sprintf("gitaly-%d", pid), fmt.Sprintf("repos-%d", groupID), "cgroup.procs")
			content := readCgroupFile(t, path)

			cmdPid, err := strconv.Atoi(string(content))
			require.NoError(t, err)

			require.Equal(t, cmd2.Process.Pid, cmdPid)
		}
	})
}

func TestCleanup(t *testing.T) {
	mock := newMock(t)

	pid := 1
	v1Manager := &CGroupV1Manager{
		cfg:       defaultCgroupsConfig(),
		hierarchy: mock.hierarchy,
		pid:       pid,
	}
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
	t.Parallel()

	mock := newMock(t)

	config := defaultCgroupsConfig()
	config.Repositories.Count = 1
	config.Repositories.MemoryBytes = 1048576
	config.Repositories.CPUShares = 16

	v1Manager1 := newV1Manager(config, 1)
	v1Manager1.hierarchy = mock.hierarchy

	mock.setupMockCgroupFiles(t, v1Manager1, 2)

	require.NoError(t, v1Manager1.Setup())

	ctx := testhelper.Context(t)

	cmd := exec.CommandContext(ctx, "ls", "-hal", ".")
	require.NoError(t, cmd.Start())
	_, err := v1Manager1.AddCommand(cmd)
	require.NoError(t, err)

	gitCmd1 := exec.CommandContext(ctx, "ls", "-hal", ".")
	require.NoError(t, gitCmd1.Start())
	_, err = v1Manager1.AddCommand(gitCmd1)
	require.NoError(t, err)

	gitCmd2 := exec.CommandContext(ctx, "ls", "-hal", ".")
	require.NoError(t, gitCmd2.Start())
	_, err = v1Manager1.AddCommand(gitCmd2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, gitCmd2.Wait())
	}()

	require.NoError(t, cmd.Wait())
	require.NoError(t, gitCmd1.Wait())

	repoCgroupPath := filepath.Join(v1Manager1.currentProcessCgroup(), "repos-0")

	expected := bytes.NewBufferString(fmt.Sprintf(`# HELP gitaly_cgroup_cpu_usage_total CPU Usage of Cgroup
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
`, repoCgroupPath, repoCgroupPath, repoCgroupPath, repoCgroupPath, repoCgroupPath))

	for _, metricsEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("metrics enabled: %v", metricsEnabled), func(t *testing.T) {
			v1Manager1.cfg.MetricsEnabled = metricsEnabled

			if metricsEnabled {
				assert.NoError(t, testutil.CollectAndCompare(
					v1Manager1,
					expected))
			} else {
				assert.NoError(t, testutil.CollectAndCompare(
					v1Manager1,
					bytes.NewBufferString("")))
			}
		})
	}
}

func readCgroupFile(t *testing.T, path string) []byte {
	t.Helper()

	// The cgroups package defaults to permission 0 as it expects the file to be existing (the kernel creates the file)
	// and its testing override the permission private variable to something sensible, hence we have to chmod ourselves
	// so we can read the file.
	require.NoError(t, os.Chmod(path, perm.PublicFile))

	return testhelper.MustReadFile(t, path)
}
