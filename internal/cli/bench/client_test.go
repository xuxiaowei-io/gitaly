package bench

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestBenchClient(t *testing.T) {
	t.Parallel()

	validRPCInfo := []byte(`{"FindCommit":{"service":"gitaly.CommitService","proto_file":"commit.proto"}}`)
	validSetup := func() string {
		tDir := testhelper.TempDir(t)
		require.NoError(t, os.MkdirAll(filepath.Join(tDir, "FindCommit"), perm.PrivateDir))
		require.NoError(t, os.WriteFile(filepath.Join(tDir, "FindCommit", "git.git.json"), []byte(`{"foo":"bar"}`), perm.PrivateFile))
		return tDir
	}

	for _, tc := range []struct {
		name            string
		setupQueries    func() string
		rpcInfo         []byte
		benchCmd        []string
		expectedActions []string
		expectedErr     string
	}{
		{
			name: "no queries",
			setupQueries: func() string {
				return testhelper.TempDir(t)
			},
			rpcInfo:         validRPCInfo,
			expectedActions: []string{exitCoordAction},
		},
		{
			name:            "invalid rpc_info.json",
			setupQueries:    validSetup,
			rpcInfo:         []byte("not valid json"),
			expectedActions: []string{exitCoordAction},
			expectedErr:     "get RPC info: unmarshaling",
		},
		{
			name: "an unknown rpc in queries",
			setupQueries: func() string {
				tDir := testhelper.TempDir(t)
				require.NoError(t, os.MkdirAll(filepath.Join(tDir, "NotAnRPC"), perm.PrivateDir))
				require.NoError(t, os.WriteFile(filepath.Join(tDir, "NotAnRPC", "git.git.json"), []byte(`{"foo":"bar"}`), perm.PrivateFile))
				return tDir
			},
			rpcInfo:         validRPCInfo,
			expectedErr:     `find queries: walking query directory: unknown RPC "NotAnRPC"`,
			expectedActions: []string{exitCoordAction},
		},
		{
			name:            "bench cmd fails",
			setupQueries:    validSetup,
			rpcInfo:         validRPCInfo,
			benchCmd:        cmdOK(false),
			expectedActions: []string{startGitalyAction, exitCoordAction},
			expectedErr:     "run bench: run ghz: exit status 1",
		},
		{
			name:            "single query",
			setupQueries:    validSetup,
			rpcInfo:         validRPCInfo,
			benchCmd:        cmdOK(true),
			expectedActions: []string{startGitalyAction, stopGitalyAction, exitCoordAction},
		},
		{
			name: "two queries",
			setupQueries: func() string {
				tDir := testhelper.TempDir(t)
				require.NoError(t, os.MkdirAll(filepath.Join(tDir, "FindCommit"), perm.PrivateDir))
				require.NoError(t, os.WriteFile(filepath.Join(tDir, "FindCommit", "git.git.json"), []byte(`{"foo":"bar"}`), perm.PrivateFile))
				require.NoError(t, os.MkdirAll(filepath.Join(tDir, "RepositorySize"), perm.PrivateDir))
				require.NoError(t, os.WriteFile(filepath.Join(tDir, "RepositorySize", "git.git.json"), []byte(`{"foo":"bar"}`), perm.PrivateFile))
				return tDir
			},
			rpcInfo:         []byte(`{"FindCommit":{"service":"gitaly.CommitService","proto_file":"commit.proto"},"RepositorySize":{"service":"gitaly.RepositoryService","proto_file":"repository.proto"}}`),
			benchCmd:        cmdOK(true),
			expectedActions: []string{startGitalyAction, stopGitalyAction, startGitalyAction, stopGitalyAction, exitCoordAction},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l, addr := testhelper.GetLocalhostListener(t)
			defer l.Close()

			queryDir := tc.setupQueries()
			infoPath := filepath.Join(queryDir, "rpc_info.json")
			require.NoError(t, os.WriteFile(infoPath, tc.rpcInfo, perm.PrivateFile))

			actionCh := make(chan ([]string), 1)
			go consumeCmds(t, l, len(tc.expectedActions), actionCh)

			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)

			client := &Client{
				ServerAddr: addr,
				RootOutDir: testhelper.TempDir(t),
				QueryDir:   queryDir,
				rw:         newJSONRW(conn),
				BenchCmd:   func(Query, string) []string { return tc.benchCmd },
			}

			err = client.run()
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedActions, <-actionCh)
		})
	}
}

func consumeCmds(t *testing.T, l net.Listener, numActions int, actionCh chan ([]string)) {
	t.Helper()

	conn, err := l.Accept()
	assert.NoError(t, err)
	defer conn.Close()

	rw := newJSONRW(conn)

	var actions []string
	for i := 0; i < numActions; i++ {
		var cmd CoordCmd
		assert.NoError(t, rw.decoder.Decode(&cmd))

		actions = append(actions, cmd.Action)

		assert.NoError(t, rw.encoder.Encode(CoordResp{}))
	}

	actionCh <- actions
}
