//go:build !gitaly_test_sha256

package cache

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	diskcache "gitlab.com/gitlab-org/gitaly/v15/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v15/internal/middleware/cache/testdata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestInvalidators(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	repo := &gitalypb.Repository{
		GitAlternateObjectDirectories: []string{"1"},
		GitObjectDirectory:            "1",
		GlProjectPath:                 "1",
		GlRepository:                  "1",
		RelativePath:                  "1",
		StorageName:                   "1",
	}

	for _, tc := range []struct {
		desc                  string
		invokeRPC             func(*testing.T, *grpc.ClientConn)
		expectedInvalidations []*gitalypb.Repository
	}{
		{
			desc: "streaming accessor does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				stream, err := testdata.NewTestServiceClient(conn).ClientStreamRepoAccessor(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)

				_, err = stream.Recv()
				require.Equal(t, err, io.EOF)
			},
		},
		{
			desc: "streaming maintainer does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				stream, err := testdata.NewTestServiceClient(conn).ClientStreamRepoMaintainer(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)

				_, err = stream.Recv()
				require.Equal(t, err, io.EOF)
			},
		},
		{
			desc: "streaming mutator invalidates cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				stream, err := testdata.NewTestServiceClient(conn).ClientStreamRepoMutator(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)

				_, err = stream.Recv()
				require.Equal(t, err, io.EOF)
			},
			expectedInvalidations: []*gitalypb.Repository{repo},
		},
		{
			desc: "unary accessor does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				_, err := testdata.NewTestServiceClient(conn).ClientUnaryRepoAccessor(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)
			},
		},
		{
			desc: "unary maintainer does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				_, err := testdata.NewTestServiceClient(conn).ClientUnaryRepoMaintainer(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)
			},
		},
		{
			desc: "unary mutator invalidates cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				_, err := testdata.NewTestServiceClient(conn).ClientUnaryRepoMutator(ctx, &testdata.Request{
					Destination: repo,
				})
				require.NoError(t, err)
			},
			expectedInvalidations: []*gitalypb.Repository{repo},
		},
		{
			desc: "health check does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{
					Service: "TestService",
				})
				require.NoError(t, err)
			},
		},
		{
			desc: "intercepted method does not invalidate cache",
			invokeRPC: func(t *testing.T, conn *grpc.ClientConn) {
				_, err := testdata.NewInterceptedServiceClient(conn).IgnoredMethod(ctx, &testdata.Request{})
				require.NoError(t, err)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mockCache := newMockCache()
			registry, err := protoregistry.NewFromPaths("middleware/cache/testdata/stream.proto")
			require.NoError(t, err)

			server := grpc.NewServer(
				grpc.StreamInterceptor(StreamInvalidator(mockCache, registry)),
				grpc.UnaryInterceptor(UnaryInvalidator(mockCache, registry)),
			)

			service := &testService{
				requestCh: make(chan bool, 1),
			}
			testdata.RegisterTestServiceServer(server, service)
			testdata.RegisterInterceptedServiceServer(server, service)
			grpc_health_v1.RegisterHealthServer(server, service)

			listener, err := net.Listen("tcp", ":0")
			require.NoError(t, err)
			go func() {
				testhelper.MustServe(t, server, listener)
			}()
			t.Cleanup(server.Stop)

			conn, err := grpc.DialContext(
				ctx,
				listener.Addr().String(),
				grpc.WithBlock(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)

			tc.invokeRPC(t, conn)

			// Check that the RPC has been executed.
			require.True(t, <-service.requestCh)
			// Verify that the repository's cache was invalidated as expected.
			testhelper.ProtoEqual(t, tc.expectedInvalidations, mockCache.invalidatedRepos)
			// Verify that all leases that were created have ended.
			require.Equal(t, len(mockCache.invalidatedRepos), mockCache.endedLeases.count)
			// And furthermore, verify that we didn't see an error.
			require.Empty(t, MethodErrCount.Method)
		})
	}
}

// mockCache allows us to relay back via channel which repos are being
// invalidated in the cache
type mockCache struct {
	invalidatedRepos []*gitalypb.Repository
	endedLeases      *struct {
		sync.RWMutex
		count int
	}
}

func newMockCache() *mockCache {
	return &mockCache{
		endedLeases: &struct {
			sync.RWMutex
			count int
		}{},
	}
}

func (mc *mockCache) EndLease(_ context.Context) error {
	mc.endedLeases.Lock()
	defer mc.endedLeases.Unlock()
	mc.endedLeases.count++

	return nil
}

func (mc *mockCache) StartLease(repo *gitalypb.Repository) (diskcache.LeaseEnder, error) {
	mc.invalidatedRepos = append(mc.invalidatedRepos, repo)
	return mc, nil
}

type testService struct {
	testdata.UnimplementedTestServiceServer
	testdata.UnimplementedInterceptedServiceServer
	grpc_health_v1.UnimplementedHealthServer
	requestCh chan bool
}

func (ts *testService) ClientStreamRepoMutator(*testdata.Request, testdata.TestService_ClientStreamRepoMutatorServer) error {
	ts.requestCh <- true
	return nil
}

func (ts *testService) ClientStreamRepoAccessor(*testdata.Request, testdata.TestService_ClientStreamRepoAccessorServer) error {
	ts.requestCh <- true
	return nil
}

func (ts *testService) ClientStreamRepoMaintainer(*testdata.Request, testdata.TestService_ClientStreamRepoMaintainerServer) error {
	ts.requestCh <- true
	return nil
}

func (ts *testService) ClientUnaryRepoMutator(context.Context, *testdata.Request) (*testdata.Response, error) {
	ts.requestCh <- true
	return &testdata.Response{}, nil
}

func (ts *testService) ClientUnaryRepoAccessor(context.Context, *testdata.Request) (*testdata.Response, error) {
	ts.requestCh <- true
	return &testdata.Response{}, nil
}

func (ts *testService) ClientUnaryRepoMaintainer(context.Context, *testdata.Request) (*testdata.Response, error) {
	ts.requestCh <- true
	return &testdata.Response{}, nil
}

func (ts *testService) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	ts.requestCh <- true
	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func (ts *testService) IgnoredMethod(context.Context, *testdata.Request) (*testdata.Response, error) {
	ts.requestCh <- true
	return &testdata.Response{}, nil
}
