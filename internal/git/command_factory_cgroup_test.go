//go:build !gitaly_test_sha256

package git_test

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

type mockCgroupsManager struct {
	cgroups.Manager
	commands []*exec.Cmd
}

func (m *mockCgroupsManager) AddCommand(c *exec.Cmd, _ ...cgroups.AddCommandOption) (string, error) {
	m.commands = append(m.commands, c)
	return "", nil
}

func TestNewCommandAddsToCgroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := CreateRepository(t, ctx, cfg, CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	for _, tc := range []struct {
		desc      string
		cgroupsFF bool
	}{
		{
			desc:      "cgroups feature flag on",
			cgroupsFF: true,
		},
		{
			desc:      "cgroups feature flag off",
			cgroupsFF: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := featureflag.IncomingCtxWithFeatureFlag(ctx, featureflag.RunCommandsInCGroup, tc.cgroupsFF)

			var manager mockCgroupsManager
			gitCmdFactory := NewCommandFactory(t, cfg, WithCgroupsManager(&manager))

			cmd, err := gitCmdFactory.New(ctx, repo, Command{
				Name: "rev-parse",
				Flags: []Option{
					Flag{Name: "--is-bare-repository"},
				},
			})
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())

			if tc.cgroupsFF {
				require.Len(t, manager.commands, 1)
				require.Contains(t, manager.commands[0].Args, "rev-parse")
				return
			}

			require.Len(t, manager.commands, 0)
		})
	}
}
