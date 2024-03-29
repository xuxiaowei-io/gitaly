package nodes

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/listenmux"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var shardName = "test-shard-0"

func TestGetPrimaryAndSecondaries(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	logger := testhelper.SharedLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	socketName := "unix://" + praefectSocket

	conf := config.Config{
		SocketPath: socketName,
		Failover:   config.Failover{Enabled: true},
	}

	internalSocket0 := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, internalSocket0)

	cc0, err := grpc.Dial(
		"unix://"+internalSocket0,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer testhelper.MustClose(t, cc0)
	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0 := promtest.NewMockHistogramVec()
	cs0 := newConnectionStatus(config.Node{Storage: storageName + "-0"}, cc0, testhelper.SharedLogger(t), mockHistogramVec0, nil)

	ns := []*nodeStatus{cs0}
	elector := newSQLElector(shardName, conf, db.DB, logger, ns)
	require.Contains(t, elector.praefectName, ":"+socketName)
	require.Equal(t, elector.shardName, shardName)
	ctx := testhelper.Context(t)
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	require.NoError(t, elector.demotePrimary(ctx, db))
	shard, err := elector.GetShard(ctx)
	db.RequireRowsInTable(t, "shard_primaries", 1)
	require.Equal(t, ErrPrimaryNotHealthy, err)
	require.Empty(t, shard)
}

func TestSqlElector_slow_execution(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	praefectSocket := "unix://" + testhelper.GetTemporaryGitalySocketFileName(t)
	logger := testhelper.SharedLogger(t).WithField("test", t.Name())

	gitalySocket := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, gitalySocket)

	gitalyConn, err := grpc.Dial(
		"unix://"+gitalySocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer testhelper.MustClose(t, gitalyConn)
	require.NoError(t, err)

	gitalyNodeStatus := newConnectionStatus(config.Node{Storage: "gitaly", Address: "gitaly-address"}, gitalyConn, logger, promtest.NewMockHistogramVec(), nil)
	elector := newSQLElector(shardName, config.Config{SocketPath: praefectSocket}, db.DB, logger, []*nodeStatus{gitalyNodeStatus})
	ctx := testhelper.Context(t)

	// Failover timeout is set to 0. If the election checks do not happen exactly at the same time
	// as when the health checks are updated, gitaly node in the test is going to be considered
	// unhealthy and the test will fail.
	elector.failoverTimeout = 0

	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	shard, err := elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{gitalyNodeStatus.GetStorage(), gitalyNodeStatus.GetAddress()},
		Secondaries: []nodeAssertion{},
	}, shard)
}

func TestBasicFailover(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	logger := testhelper.SharedLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	socketName := "unix://" + praefectSocket

	conf := config.Config{SocketPath: socketName}

	internalSocket0, internalSocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	healthSrv0 := testhelper.NewServerWithHealth(t, internalSocket0)
	healthSrv1 := testhelper.NewServerWithHealth(t, internalSocket1)

	addr0 := "unix://" + internalSocket0
	cc0, err := grpc.Dial(
		addr0,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer testhelper.MustClose(t, cc0)
	require.NoError(t, err)

	addr1 := "unix://" + internalSocket1
	cc1, err := grpc.Dial(
		addr1,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer testhelper.MustClose(t, cc1)

	require.NoError(t, err)

	storageName := "default"

	cs0 := newConnectionStatus(config.Node{Storage: storageName + "-0", Address: addr0}, cc0, logger, promtest.NewMockHistogramVec(), nil)
	cs1 := newConnectionStatus(config.Node{Storage: storageName + "-1", Address: addr1}, cc1, logger, promtest.NewMockHistogramVec(), nil)

	ns := []*nodeStatus{cs0, cs1}
	elector := newSQLElector(shardName, conf, db.DB, logger, ns)
	ctx := testhelper.Context(t)
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	require.Equal(t, cs0, elector.primaryNode.Node)
	shard, err := elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring first node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	predateElection(t, ctx, db, shardName, failoverTimeout)

	// Primary should remain before the failover timeout is exceeded
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Predate the timeout to exceed it
	predateLastSeenActiveAt(t, db, shardName, cs0.GetStorage(), failoverTimeout)

	// Expect that the other node is promoted
	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)
	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs1.GetStorage(), cs1.GetAddress()},
		Secondaries: []nodeAssertion{{cs0.GetStorage(), cs0.GetAddress()}},
	}, shard)

	// Failover back to the original node
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	predateElection(t, ctx, db, shardName, failoverTimeout)
	predateLastSeenActiveAt(t, db, shardName, cs1.GetStorage(), failoverTimeout)
	require.NoError(t, elector.checkNodes(ctx))

	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring second node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	predateElection(t, ctx, db, shardName, failoverTimeout)
	predateLastSeenActiveAt(t, db, shardName, cs0.GetStorage(), failoverTimeout)

	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	db.RequireRowsInTable(t, "node_status", 2)
	// No new candidates
	_, err = elector.GetShard(ctx)
	require.Equal(t, ErrPrimaryNotHealthy, err)
}

func TestElectDemotedPrimary(t *testing.T) {
	t.Parallel()
	tx := testdb.New(t).Begin(t)
	defer tx.Rollback(t)

	node := config.Node{Storage: "gitaly-0"}
	elector := newSQLElector(
		shardName,
		config.Config{},
		nil,
		testhelper.SharedLogger(t),
		[]*nodeStatus{{node: node}},
	)
	ctx := testhelper.Context(t)

	candidates := []*sqlCandidate{{Node: &nodeStatus{node: node}}}
	require.NoError(t, elector.electNewPrimary(ctx, tx.Tx, candidates))

	primary, err := elector.lookupPrimary(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())

	require.NoError(t, elector.demotePrimary(ctx, tx))

	primary, err = elector.lookupPrimary(ctx, tx)
	require.NoError(t, err)
	require.Nil(t, primary)

	predateElection(t, ctx, tx, shardName, failoverTimeout+time.Microsecond)
	require.NoError(t, err)
	require.NoError(t, elector.electNewPrimary(ctx, tx.Tx, candidates))

	primary, err = elector.lookupPrimary(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())
}

// predateLastSeenActiveAt shifts the last_seen_active_at column to an earlier time. This avoids
// waiting for the node's status to become unhealthy.
func predateLastSeenActiveAt(tb testing.TB, db testdb.DB, shardName, nodeName string, amount time.Duration) {
	tb.Helper()

	_, err := db.Exec(`
UPDATE node_status SET last_seen_active_at = last_seen_active_at - INTERVAL '1 MICROSECOND' * $1
WHERE shard_name = $2 AND node_name = $3`, amount.Microseconds(), shardName, nodeName,
	)

	require.NoError(tb, err)
}

// predateElection shifts the election to an earlier time. This avoids waiting for the failover timeout to trigger
// a new election.
func predateElection(tb testing.TB, ctx context.Context, db glsql.Querier, shardName string, amount time.Duration) {
	tb.Helper()

	_, err := db.ExecContext(ctx,
		"UPDATE shard_primaries SET elected_at = elected_at - INTERVAL '1 MICROSECOND' * $1 WHERE shard_name = $2",
		amount.Microseconds(),
		shardName,
	)

	require.NoError(tb, err)
}

func TestElectNewPrimary(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	ns := []*nodeStatus{{
		node: config.Node{
			Storage: "gitaly-0",
		},
	}, {
		node: config.Node{
			Storage: "gitaly-1",
		},
	}, {
		node: config.Node{
			Storage: "gitaly-2",
		},
	}}

	candidates := []*sqlCandidate{
		{
			&nodeStatus{
				node: config.Node{
					Storage: "gitaly-1",
				},
			},
		}, {
			&nodeStatus{
				node: config.Node{
					Storage: "gitaly-2",
				},
			},
		},
	}

	testCases := []struct {
		desc                   string
		initialReplQueueInsert string
		expectedPrimary        string
		fallbackChoice         bool
	}{
		{
			desc: "gitaly-2 storage has more up to date repositories",
			initialReplQueueInsert: `
			INSERT INTO repositories
				(repository_id, virtual_storage, relative_path, generation)
			VALUES
				(1, 'test-shard-0', '/p/1', 5),
				(2, 'test-shard-0', '/p/2', 5),
				(3, 'test-shard-0', '/p/3', 5),
				(4, 'test-shard-0', '/p/4', 5),
				(5, 'test-shard-0', '/p/5', 5);

			INSERT INTO storage_repositories
				(repository_id, virtual_storage, relative_path, storage, generation)
			VALUES
				(1, 'test-shard-0', '/p/1', 'gitaly-1', 5),
				(2, 'test-shard-0', '/p/2', 'gitaly-1', 5),
				(3, 'test-shard-0', '/p/3', 'gitaly-1', 4),
				(4, 'test-shard-0', '/p/4', 'gitaly-1', 3),

				(1, 'test-shard-0', '/p/1', 'gitaly-2', 5),
				(2, 'test-shard-0', '/p/2', 'gitaly-2', 5),
				(3, 'test-shard-0', '/p/3', 'gitaly-2', 4),
				(4, 'test-shard-0', '/p/4', 'gitaly-2', 4),
				(5, 'test-shard-0', '/p/5', 'gitaly-2', 5)
			`,
			expectedPrimary: "gitaly-2",
			fallbackChoice:  false,
		},
		{
			desc: "gitaly-2 storage has less repositories as some may not been replicated yet",
			initialReplQueueInsert: `
			INSERT INTO REPOSITORIES
				(repository_id, virtual_storage, relative_path, generation)
			VALUES
				(1, 'test-shard-0', '/p/1', 5),
				(2, 'test-shard-0', '/p/2', 5);

			INSERT INTO STORAGE_REPOSITORIES
				(repository_id, virtual_storage, relative_path, storage, generation)
			VALUES
				(1, 'test-shard-0', '/p/1', 'gitaly-1', 5),
				(2, 'test-shard-0', '/p/2', 'gitaly-1', 4),
				(1, 'test-shard-0', '/p/1', 'gitaly-2', 5)`,
			expectedPrimary: "gitaly-1",
			fallbackChoice:  false,
		},
		{
			desc: "gitaly-1 is primary as it has less generations behind in total despite it has less repositories",
			initialReplQueueInsert: `
			INSERT INTO REPOSITORIES
				(repository_id, virtual_storage, relative_path, generation)
			VALUES
				(1, 'test-shard-0', '/p/1', 2),
				(2, 'test-shard-0', '/p/2', 2),
				(3, 'test-shard-0', '/p/3', 10);

			INSERT INTO STORAGE_REPOSITORIES
				(repository_id, virtual_storage, relative_path, storage, generation)
			VALUES
				(2, 'test-shard-0', '/p/2', 'gitaly-1', 1),
				(3, 'test-shard-0', '/p/3', 'gitaly-1', 9),
				(1, 'test-shard-0', '/p/1', 'gitaly-2', 1),
				(2, 'test-shard-0', '/p/2', 'gitaly-2', 1),
				(3, 'test-shard-0', '/p/3', 'gitaly-2', 1)`,
			expectedPrimary: "gitaly-1",
			fallbackChoice:  false,
		},
		{
			desc:            "no information about generations results to first candidate",
			expectedPrimary: "gitaly-1",
			fallbackChoice:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			tx := db.Begin(t)
			defer tx.Rollback(t)

			_, err := tx.Exec(testCase.initialReplQueueInsert)
			require.NoError(t, err)

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			elector := newSQLElector(shardName, config.Config{}, db.DB, logger, ns)
			ctx := testhelper.Context(t)

			require.NoError(t, elector.electNewPrimary(ctx, tx.Tx, candidates))
			primary, err := elector.lookupPrimary(ctx, tx)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedPrimary, primary.GetStorage())

			fallbackChoice := hook.LastEntry().Data["fallback_choice"].(bool)
			require.Equal(t, testCase.fallbackChoice, fallbackChoice)
		})
	}
}

func TestConnectionMultiplexing(t *testing.T) {
	t.Parallel()
	errNonMuxed := status.Error(codes.Internal, "non-muxed connection")
	errMuxed := status.Error(codes.Internal, "muxed connection")

	logger := testhelper.SharedLogger(t)

	lm := listenmux.New(insecure.NewCredentials())
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))

	srv := grpc.NewServer(
		grpc.Creds(lm),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			_, err := backchannel.GetPeerID(stream.Context())
			if errors.Is(err, backchannel.ErrNonMultiplexedConnection) {
				return errNonMuxed
			}

			assert.NoError(t, err)
			return errMuxed
		}),
	)

	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	defer srv.Stop()

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go testhelper.MustServe(t, srv, ln)

	db := testdb.New(t)
	mgr, err := NewManager(
		testhelper.SharedLogger(t),
		config.Config{
			Failover: config.Failover{
				Enabled:          true,
				ElectionStrategy: config.ElectionStrategySQL,
			},
			VirtualStorages: []*config.VirtualStorage{
				{
					Name: "virtual-storage-1",
					Nodes: []*config.Node{
						{Storage: "storage-1", Address: "tcp://" + ln.Addr().String()},
						{Storage: "storage-2", Address: "tcp://" + ln.Addr().String()},
					},
				},
			},
		},
		db.DB,
		nil,
		promtest.NewMockHistogramVec(),
		protoregistry.GitalyProtoPreregistered,
		nil,
		backchannel.NewClientHandshaker(logger, func() backchannel.Server { return grpc.NewServer() }, backchannel.DefaultConfiguration()),
		nil,
	)
	require.NoError(t, err)
	defer mgr.Stop()

	// check the shard to get the primary in a healthy state
	mgr.checkShards()
	ctx := testhelper.Context(t)
	for _, tc := range []struct {
		desc  string
		error error
	}{
		{
			desc:  "multiplexed",
			error: errMuxed,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			shard, err := mgr.GetShard(ctx, "virtual-storage-1")
			require.NoError(t, err)
			require.Len(t, shard.Secondaries, 1)

			for _, node := range []Node{shard.Primary, shard.Secondaries[0]} {
				testhelper.RequireGrpcError(t,
					tc.error,
					node.GetConnection().Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}),
				)
			}

			nodes := mgr.Nodes()["virtual-storage-1"]
			require.Len(t, nodes, 2)
			for _, node := range nodes {
				testhelper.RequireGrpcError(t,
					tc.error,
					node.GetConnection().Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}),
				)
			}
		})
	}
}
