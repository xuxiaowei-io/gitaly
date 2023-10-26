package cgroups

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestCloneIntoCgroup(t *testing.T) {
	mountPoint := t.TempDir()
	hierarchyRoot := filepath.Join(mountPoint, "gitaly")

	// Create the files we expect the manager to open.
	require.NoError(t, os.MkdirAll(filepath.Join(hierarchyRoot, "gitaly-1"), fs.ModePerm))
	require.NoError(t, os.MkdirAll(filepath.Join(hierarchyRoot, "gitaly-1", "repos-3"), fs.ModePerm))
	require.NoError(t, os.MkdirAll(filepath.Join(hierarchyRoot, "gitaly-1", "repos-5"), fs.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(hierarchyRoot, "gitaly-1", "cgroup.subtree_control"), nil, fs.ModePerm))

	mgr := NewManager(cgroups.Config{
		Mountpoint:    mountPoint,
		HierarchyRoot: "gitaly",
		Repositories: cgroups.Repositories{
			Count: 10,
		},
	}, testhelper.NewLogger(t), 1)

	t.Run("command args used as key", func(t *testing.T) {
		for _, tc := range []struct {
			desc                string
			existingSysProcAttr *syscall.SysProcAttr
			expectedSysProcAttr *syscall.SysProcAttr
		}{
			{
				desc: "nil SysProcAttr",
				expectedSysProcAttr: &syscall.SysProcAttr{
					UseCgroupFD: true,
				},
			},
			{
				desc: "existing SysProcAttr",
				existingSysProcAttr: &syscall.SysProcAttr{
					Chroot: "preserved",
				},
				expectedSysProcAttr: &syscall.SysProcAttr{
					Chroot:      "preserved",
					UseCgroupFD: true,
				},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				command := exec.Command("command", "arg")
				command.SysProcAttr = tc.existingSysProcAttr

				path, closer, err := mgr.CloneIntoCgroup(command)
				require.NoError(t, err)
				defer testhelper.MustClose(t, closer)
				require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-5"), path)

				// CgroupFD may vary so just set it in the assertion after verifying it is set.
				require.NotEqual(t, 0, command.SysProcAttr.UseCgroupFD)
				tc.expectedSysProcAttr.CgroupFD = command.SysProcAttr.CgroupFD

				require.Equal(t, tc.expectedSysProcAttr, command.SysProcAttr)
			})
		}
	})

	t.Run("key provided", func(t *testing.T) {
		commandWithKey := exec.Command("command", "arg")
		pathWithKey, closeWithKey, err := mgr.CloneIntoCgroup(commandWithKey, WithCgroupKey("some-key"))
		require.NoError(t, err)
		defer testhelper.MustClose(t, closeWithKey)
		require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-3"), pathWithKey)
		require.True(t, commandWithKey.SysProcAttr.UseCgroupFD)
		require.NotEqual(t, 0, commandWithKey.SysProcAttr.UseCgroupFD)

		commandWithoutKey := exec.Command("command", "arg")
		pathWithoutKey, closeWithoutKey, err := mgr.CloneIntoCgroup(commandWithoutKey)
		require.NoError(t, err)
		defer testhelper.MustClose(t, closeWithoutKey)
		require.Equal(t, filepath.Join(hierarchyRoot, "gitaly-1/repos-5"), pathWithoutKey)
		require.True(t, commandWithoutKey.SysProcAttr.UseCgroupFD)
		require.NotEqual(t, 0, commandWithoutKey.SysProcAttr.UseCgroupFD)

		require.NotEqual(t, pathWithKey, pathWithoutKey, "commands should be placed in different groups")
		require.NotEqual(t, commandWithKey.SysProcAttr.CgroupFD, commandWithoutKey.SysProcAttr.CgroupFD)
	})
}

func TestNewCgroupStatus(t *testing.T) {
	cfg := cgroups.Config{Repositories: cgroups.Repositories{Count: 1000}}
	repoPath := func(i int) string {
		return filepath.Join("gitaly/gitaly-1", fmt.Sprintf("repos-%d", i))
	}

	status := newCgroupStatus(cfg, repoPath)

	for i := 0; i < int(cfg.Repositories.Count); i++ {
		cgroupPath := repoPath(i)
		cgLock, ok := status.m[cgroupPath]
		require.True(t, ok)
		require.NotNil(t, cgLock)
		require.False(t, cgLock.isCreated())
	}
}
