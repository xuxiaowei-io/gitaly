package bench

import (
	"net"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func cmdOK(ok bool) []string {
	prefix := "/bin"
	if runtime.GOOS == "darwin" {
		prefix = "/usr" + prefix
	}

	if ok {
		return []string{prefix + "/true"}
	}
	return []string{prefix + "/false"}
}

func TestBenchCoordinator(t *testing.T) {
	t.Parallel()

	outDir := "/tmp/out-dir"

	for _, tc := range []struct {
		name          string
		startCmd      []string
		stopCmd       []string
		incomingCmds  []CoordCmd
		expectedResps []CoordResp
		expectedErr   string
	}{
		{
			name:     "no jobs",
			startCmd: cmdOK(true),
			stopCmd:  cmdOK(true),
			incomingCmds: []CoordCmd{
				{
					Action: exitCoordAction,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "",
				},
			},
		},
		{
			name:     "start gitaly fails",
			startCmd: cmdOK(false),
			stopCmd:  cmdOK(false),
			incomingCmds: []CoordCmd{
				{
					Action: startGitalyAction,
					OutDir: outDir,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "start benchmarking: start gitaly: exit status 1",
				},
			},
			expectedErr: "coordinator: session: start benchmarking: start gitaly: exit status 1",
		},
		{
			name:     "stop gitaly fails",
			startCmd: cmdOK(true),
			stopCmd:  cmdOK(false),
			incomingCmds: []CoordCmd{
				{
					Action: startGitalyAction,
					OutDir: outDir,
				},
				{
					Action: stopGitalyAction,
					OutDir: outDir,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "",
				},
				{
					Error: "finish benchmarking: stop gitaly: exit status 1",
				},
			},
			expectedErr: "coordinator: session: finish benchmarking: stop gitaly: exit status 1",
		},
		{
			name:     "stop without start",
			startCmd: cmdOK(true),
			stopCmd:  cmdOK(false),
			incomingCmds: []CoordCmd{
				{
					Action: stopGitalyAction,
					OutDir: outDir,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "received 'stop' command when Gitaly was not running",
				},
			},
			expectedErr: "coordinator: session: received 'stop' command when Gitaly was not running",
		},
		{
			name:     "double start",
			startCmd: cmdOK(true),
			stopCmd:  cmdOK(false),
			incomingCmds: []CoordCmd{
				{
					Action: startGitalyAction,
					OutDir: outDir,
				},
				{
					Action: startGitalyAction,
					OutDir: outDir,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "",
				},
				{
					Error: "finish benchmarking: received command other than 'stop' while Gitaly was running",
				},
			},
			expectedErr: "coordinator: session: finish benchmarking: received command other than 'stop' while Gitaly was running",
		},
		{
			name:     "ok",
			startCmd: cmdOK(true),
			stopCmd:  cmdOK(true),
			incomingCmds: []CoordCmd{
				{
					Action: startGitalyAction,
					OutDir: outDir,
				},
				{
					Action: stopGitalyAction,
					OutDir: outDir,
				},
				{
					Action: exitCoordAction,
				},
			},
			expectedResps: []CoordResp{
				{
					Error: "",
				},
				{
					Error: "",
				},
				{
					Error: "",
				},
			},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l, addr := testhelper.GetLocalhostListener(t)
			defer l.Close()

			coord := &Coordinator{
				Listener: l,
				StartCmd: tc.startCmd,
				StopCmd:  tc.stopCmd,
			}

			respCh := make(chan ([]CoordResp), 1)
			go sendCmds(t, addr, tc.incomingCmds, respCh)

			if tc.expectedErr != "" {
				require.ErrorContains(t, coord.run(), tc.expectedErr)
			} else {
				require.NoError(t, coord.run())
			}

			responses := <-respCh
			require.Equal(t, tc.expectedResps, responses)
		})
	}
}

func sendCmds(t *testing.T, addr string, cmds []CoordCmd, outCh chan ([]CoordResp)) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer conn.Close()

	rw := newJSONRW(conn)

	var resps []CoordResp
	for _, cmd := range cmds {
		assert.NoError(t, rw.encoder.Encode(cmd))

		var resp CoordResp
		assert.NoError(t, rw.decoder.Decode(&resp))

		resps = append(resps, resp)
	}

	outCh <- resps
}
