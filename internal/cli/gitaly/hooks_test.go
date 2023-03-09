package gitaly

import (
	"bytes"
	"io"
	"io/fs"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v15/auth"
	gclient "gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc"
)

// This test cannot be made to run in parallel because it relies on setting the
// umask which is unfortunately not thread safe.
func TestSetHooksSubcommand(t *testing.T) {
	ctx := testhelper.Context(t)
	umask := perm.GetUmask()

	cfg := testcfg.Build(t)
	testcfg.BuildGitaly(t, cfg)

	serverSocketPath := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	repoCfg := cfg
	repoCfg.SocketPath = serverSocketPath

	// The generated socket path already has the unix prefix. This needs to be
	// removed because the Gitaly config does not expect a scheme to be present.
	cfg.SocketPath = strings.TrimPrefix(serverSocketPath, "unix://")
	client := newRepositoryClient(t, cfg, serverSocketPath)

	configPath := testcfg.WriteTemporaryGitalyConfigFile(t, cfg)

	for _, tc := range []struct {
		desc          string
		setup         func() ([]string, *gitalypb.Repository)
		hooks         io.Reader
		error         string
		expectedState testhelper.DirectoryState
	}{
		{
			desc: "missing repository flag",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=" + repo.StorageName, "--config=" + configPath}, repo
			},
			hooks: &bytes.Buffer{},
			error: "Required flag \"repository\" not set\n",
		},
		{
			desc: "missing config flag",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=" + repo.StorageName, "--repository=" + repo.RelativePath}, repo
			},
			hooks: &bytes.Buffer{},
			error: "Required flag \"c\" not set\n",
		},
		{
			desc: "virtual storage not found",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=non-existent", "--repository=" + repo.RelativePath, "--config=" + configPath}, repo
			},
			hooks: testhelper.MustCreateCustomHooksTar(t),
			error: testhelper.GitalyOrPraefect(
				"getting repo path: GetStorageByName: no such storage: \"non-existent\"\n",
				"rpc error: code = InvalidArgument desc = repo scoped: invalid Repository\n",
			),
		},
		{
			desc: "repository not found",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=" + repo.StorageName, "--repository=non-existent", "--config=" + configPath}, repo
			},
			hooks: testhelper.MustCreateCustomHooksTar(t),
			error: testhelper.GitalyOrPraefect(
				"getting repo path: GetRepoPath: not a git repository:",
				"rpc error: code = NotFound desc = mutator call: route repository mutator: get repository id: repository \"default\"/\"non-existent\" not found\n",
			),
		},
		{
			desc: "successfully set with empty hooks",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=" + repo.StorageName, "--repository=" + repo.RelativePath, "--config=" + configPath}, repo
			},
			hooks: &bytes.Buffer{},
			expectedState: testhelper.DirectoryState{
				"custom_hooks/": {Mode: umask.Mask(fs.ModePerm)},
			},
		},
		{
			desc: "successfully set with hooks",
			setup: func() ([]string, *gitalypb.Repository) {
				repo, _ := gittest.CreateRepository(t, ctx, repoCfg)
				return []string{"--storage=" + repo.StorageName, "--repository=" + repo.RelativePath, "--config=" + configPath}, repo
			},
			hooks: testhelper.MustCreateCustomHooksTar(t),
			expectedState: testhelper.DirectoryState{
				"custom_hooks/":            {Mode: umask.Mask(fs.ModePerm)},
				"custom_hooks/pre-commit":  {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-commit content")},
				"custom_hooks/pre-push":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-push content")},
				"custom_hooks/pre-receive": {Mode: umask.Mask(fs.ModePerm), Content: []byte("pre-receive content")},
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			flags, repo := tc.setup()

			args := append([]string{"hooks", "set"}, flags...)
			cmd := exec.Command(cfg.BinaryPath("gitaly"), args...)

			var stderr bytes.Buffer
			cmd.Stdin = tc.hooks
			cmd.Stderr = &stderr

			err := cmd.Run()

			if tc.error != "" {
				require.Error(t, err)
				require.Contains(t, stderr.String(), tc.error)
				require.False(t, cmd.ProcessState.Success())
			} else {
				require.Empty(t, stderr.String())
				require.NoError(t, err)
				require.True(t, cmd.ProcessState.Success())
			}

			stream, err := client.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{
				Repository: &gitalypb.Repository{
					StorageName:  repo.StorageName,
					RelativePath: repo.RelativePath,
				},
			})
			require.NoError(t, err)

			hooksReader := streamio.NewReader(func() ([]byte, error) {
				response, err := stream.Recv()
				return response.GetData(), err
			})

			testhelper.RequireTarState(t, hooksReader, tc.expectedState)
		})
	}
}

func newRepositoryClient(tb testing.TB, cfg config.Cfg, serverSocketPath string) gitalypb.RepositoryServiceClient {
	tb.Helper()

	connOpts := []grpc.DialOption{
		internalclient.UnaryInterceptor(), internalclient.StreamInterceptor(),
	}
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := gclient.Dial(serverSocketPath, connOpts)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}
