//go:build !gitaly_test_sha256

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestConnectivity(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	repo, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	cwd, err := os.Getwd()
	require.NoError(t, err)

	tempDir := testhelper.TempDir(t)

	relativeSocketPath, err := filepath.Rel(cwd, filepath.Join(tempDir, "gitaly.socket"))
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(relativeSocketPath))
	require.NoError(t, os.Symlink(cfg.SocketPath, relativeSocketPath))

	runGitaly := func(tb testing.TB, cfg config.Cfg) string {
		tb.Helper()
		return testserver.RunGitalyServer(tb, cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	}

	testCases := []struct {
		name  string
		addr  func(t *testing.T, cfg config.Cfg) (string, string)
		proxy bool
	}{
		{
			name: "tcp",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				cfg.ListenAddr = "localhost:0"
				return runGitaly(t, cfg), ""
			},
		},
		{
			name: "unix absolute",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				return runGitaly(t, cfg), ""
			},
		},
		{
			name: "unix abs with proxy",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				return runGitaly(t, cfg), ""
			},
			proxy: true,
		},
		{
			name: "unix relative",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg), ""
			},
		},
		{
			name: "unix relative with proxy",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg), ""
			},
			proxy: true,
		},
		{
			name: "tls",
			addr: func(t *testing.T, cfg config.Cfg) (string, string) {
				certFile, keyFile := testhelper.GenerateCerts(t)
				t.Setenv(x509.SSLCertFile, certFile)

				cfg.TLSListenAddr = "localhost:0"
				cfg.TLS = config.TLS{
					CertPath: certFile,
					KeyPath:  keyFile,
				}
				return runGitaly(t, cfg), certFile
			},
		},
	}

	payload, err := protojson.Marshal(&gitalypb.SSHUploadPackRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			addr, certFile := testcase.addr(t, cfg)

			env := []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=%s", addr),
				fmt.Sprintf("GITALY_WD=%s", cwd),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", cfg.BinaryPath("gitaly-ssh")),
				fmt.Sprintf("SSL_CERT_FILE=%s", certFile),
			}
			if testcase.proxy {
				env = append(env,
					"http_proxy=http://invalid:1234",
					"https_proxy=https://invalid:1234",
				)
			}

			output := gittest.ExecOpts(t, cfg, gittest.ExecConfig{
				Env: env,
			}, "ls-remote", "git@localhost:test/test.git", "refs/heads/master")
			require.True(t, strings.HasSuffix(strings.TrimSpace(string(output)), "refs/heads/master"))
		})
	}
}
