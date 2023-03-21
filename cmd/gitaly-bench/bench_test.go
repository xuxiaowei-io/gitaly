package main

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestGitalyBenchCLI(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyBench(t, cfg)

	for _, tc := range []struct {
		name     string
		args     []string
		stdout   string
		exitCode int
	}{
		{
			name:     "no args",
			stdout:   "NAME:\n   gitaly-bench - coordinate starting and stopping Gitaly for benchmarking\n\nUSAGE:\n   gitaly-bench [global options] command [command options] [arguments...]\n\nCOMMANDS:\n   coordinate  coordinate starting and stopping Gitaly for benchmarking\n   client      send gRPC requests to Gitaly for benchmarking\n   help, h     Shows a list of commands or help for one command\n\nGLOBAL OPTIONS:\n   --help, -h  show help\n",
			exitCode: 0,
		},
		{
			name:     "client no args",
			args:     []string{"client"},
			stdout:   "NAME:\n   gitaly-bench client - send gRPC requests to Gitaly for benchmarking\n\nUSAGE:\n   gitaly-bench client [command options] [arguments...]\n\nDESCRIPTION:\n   Run on a client machine to benchmark RPC performance and order the coordinator to start/stop Gitaly.\n\nOPTIONS:\n   --out-dir value      Directory to write results to\n   --server-addr value  Address of Gitaly server\n   --coord-port value   TCP port coordinator is listening on (default: \"7075\")\n   --query-dir value    Path to directory containing queries to execute (default: \"/opt/ghz/queries\")\n   --help, -h           show help\n",
			exitCode: 1,
		},
		{
			name:     "coordinator no args",
			args:     []string{"coordinate"},
			stdout:   "NAME:\n   gitaly-bench coordinate - coordinate starting and stopping Gitaly for benchmarking\n\nUSAGE:\n   gitaly-bench coordinate [command options] [arguments...]\n\nDESCRIPTION:\n   Handle starting and stopping Gitaly for benchmarking, directed by the client. This will listen on a separate TCP port from Gitaly.\n\nOPTIONS:\n   --repo-dir value    Directory containing git repositories\n   --coord-port value  TCP port for coordinator to listen on (default: \"7075\")\n   --help, -h          show help\n",
			exitCode: 1,
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stderr, stdout bytes.Buffer
			cmd := exec.Command(cfg.BinaryPath("gitaly-bench"), tc.args...)
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()

			if tc.exitCode == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.exitCode, err.(*exec.ExitError).ExitCode())
			}

			require.Contains(t, stdout.String(), tc.stdout)
		})
	}
}
