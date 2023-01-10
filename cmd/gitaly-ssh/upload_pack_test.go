//go:build !gitaly_test_sha256

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

const keepAroundNamespace = "refs/keep-around"

func TestVisibilityOfHiddenRefs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	address := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	// Create a keep-around ref
	existingSha := git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	keepAroundRef := fmt.Sprintf("%s/%s", keepAroundNamespace, existingSha)

	localRepo := localrepo.NewTestRepo(t, cfg, repo)
	updater, err := updateref.New(ctx, localRepo)
	defer testhelper.MustClose(t, updater)

	require.NoError(t, err)
	require.NoError(t, updater.Start())
	require.NoError(t, updater.Create(git.ReferenceName(keepAroundRef), existingSha))
	require.NoError(t, updater.Commit())

	gittest.Exec(t, cfg, "-C", repoPath, "config", "transfer.hideRefs", keepAroundNamespace)

	output := gittest.Exec(t, cfg, "ls-remote", repoPath, keepAroundNamespace)
	require.Empty(t, output, "there should be no keep-around refs in normal ls-remote output")

	wd, err := os.Getwd()
	require.NoError(t, err)

	tests := []struct {
		name             string
		GitConfigOptions []string
		HiddenRefFound   bool
	}{
		{
			name:             "With no custom GitConfigOptions passed",
			GitConfigOptions: []string{},
			HiddenRefFound:   true,
		},
		{
			name:             "With custom GitConfigOptions passed",
			GitConfigOptions: []string{fmt.Sprintf("transfer.hideRefs=%s", keepAroundRef)},
			HiddenRefFound:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := protojson.Marshal(&gitalypb.SSHUploadPackRequest{
				Repository:       repo,
				GitConfigOptions: test.GitConfigOptions,
			})

			require.NoError(t, err)

			stdout := gittest.ExecOpts(t, cfg, gittest.ExecConfig{
				Env: []string{
					fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
					fmt.Sprintf("GITALY_ADDRESS=unix:%s", strings.TrimPrefix(address, "unix://")),
					fmt.Sprintf("GITALY_WD=%s", wd),
					fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
					fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", cfg.BinaryPath("gitaly-ssh")),
				},
			}, "ls-remote", fmt.Sprintf("%s:%s", "git@localhost", repoPath), keepAroundRef)

			if test.HiddenRefFound {
				require.Equal(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), string(stdout))
			} else {
				require.NotEqual(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), string(stdout))
			}
		})
	}
}
