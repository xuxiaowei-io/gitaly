package git_test

import (
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

type mockCgroupsManager struct {
	cgroups.Manager
	commands []*exec.Cmd
}

func (m *mockCgroupsManager) AddCommand(c *exec.Cmd, _ ...cgroups.AddCommandOption) (string, error) {
	m.commands = append(m.commands, c)
	return "", nil
}

func (m *mockCgroupsManager) SupportsCloneIntoCgroup() bool {
	return true
}

func (m *mockCgroupsManager) CloneIntoCgroup(c *exec.Cmd, _ ...cgroups.AddCommandOption) (string, io.Closer, error) {
	m.commands = append(m.commands, c)
	return "", io.NopCloser(nil), nil
}

func TestNewCommandAddsToCgroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	var manager mockCgroupsManager
	gitCmdFactory := gittest.NewCommandFactory(t, cfg, git.WithCgroupsManager(&manager))

	cmd, err := gitCmdFactory.New(ctx, repo, git.Command{
		Name: "rev-parse",
		Flags: []git.Option{
			git.Flag{Name: "--is-bare-repository"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	require.Len(t, manager.commands, 1)
	require.Contains(t, manager.commands[0].Args, "rev-parse")
}
