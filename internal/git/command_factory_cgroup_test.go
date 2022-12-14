//go:build !gitaly_test_sha256

package git_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

type mockCgroupsManager struct {
	commands []*command.Command
}

func (m *mockCgroupsManager) Setup() error {
	return nil
}

func (m *mockCgroupsManager) AddCommand(c *command.Command, repo repository.GitRepo) (string, error) {
	m.commands = append(m.commands, c)
	return "", nil
}

func (m *mockCgroupsManager) Cleanup() error {
	return nil
}

func (m *mockCgroupsManager) Collect(ch chan<- prometheus.Metric) {}
func (m *mockCgroupsManager) Describe(ch chan<- *prometheus.Desc) {}

func TestNewCommandAddsToCgroup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
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
			gitCmdFactory := gittest.NewCommandFactory(t, cfg, git.WithCgroupsManager(&manager))

			cmd, err := gitCmdFactory.New(ctx, repo, git.SubCmd{
				Name: "rev-parse",
				Flags: []git.Option{
					git.Flag{Name: "--is-bare-repository"},
				},
			})
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())

			if tc.cgroupsFF {
				require.Len(t, manager.commands, 1)
				require.Contains(t, manager.commands[0].Args(), "rev-parse")
				return
			}

			require.Len(t, manager.commands, 0)
		})
	}
}
