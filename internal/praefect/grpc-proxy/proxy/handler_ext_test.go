//go:build !gitaly_test_sha256

// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/grpc-proxy/proxy"
	pb "gitlab.com/gitlab-org/gitaly/v15/internal/praefect/grpc-proxy/testdata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpc_metadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	pingDefaultValue   = "I like kittens."
	clientMdKey        = "test-client-header"
	serverHeaderMdKey  = "test-client-header"
	serverTrailerMdKey = "test-client-trailer"

	rejectingMdKey = "test-reject-rpc-if-in-context"

	countListResponses = 20
)

// asserting service is implemented on the server side and serves as a handler for stuff
type assertingService struct {
	pb.UnimplementedTestServiceServer
	t *testing.T
}

func (s *assertingService) PingEmpty(ctx context.Context, _ *pb.Empty) (*pb.PingResponse, error) {
	// Check that this call has client's metadata.
	md, ok := grpc_metadata.FromIncomingContext(ctx)
	assert.True(s.t, ok, "PingEmpty call must have metadata in context")
	_, ok = md[clientMdKey]
	assert.True(s.t, ok, "PingEmpty call must have clients's custom headers in metadata")
	return &pb.PingResponse{Value: pingDefaultValue, Counter: 42}, nil
}

func (s *assertingService) Ping(ctx context.Context, ping *pb.PingRequest) (*pb.PingResponse, error) {
	// Send user trailers and headers.
	require.NoError(s.t, grpc.SendHeader(ctx, grpc_metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	require.NoError(s.t, grpc.SetTrailer(ctx, grpc_metadata.Pairs(serverTrailerMdKey, "I like ending turtles.")))
	return &pb.PingResponse{Value: ping.Value, Counter: 42}, nil
}

func (s *assertingService) PingError(ctx context.Context, ping *pb.PingRequest) (*pb.Empty, error) {
	return nil, status.Errorf(codes.ResourceExhausted, "Userspace error.")
}

func (s *assertingService) PingList(ping *pb.PingRequest, stream pb.TestService_PingListServer) error {
	// Send user trailers and headers.
	require.NoError(s.t, stream.SendHeader(grpc_metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	for i := 0; i < countListResponses; i++ {
		require.NoError(s.t, stream.Send(&pb.PingResponse{Value: ping.Value, Counter: int32(i)}))
	}
	stream.SetTrailer(grpc_metadata.Pairs(serverTrailerMdKey, "I like ending turtles."))
	return nil
}

func (s *assertingService) PingStream(stream pb.TestService_PingStreamServer) error {
	require.NoError(s.t, stream.SendHeader(grpc_metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	counter := int32(0)
	for {
		ping, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			require.NoError(s.t, err, "can't fail reading stream")
			return err
		}
		pong := &pb.PingResponse{Value: ping.Value, Counter: counter}
		if err := stream.Send(pong); err != nil {
			require.NoError(s.t, err, "can't fail sending back a pong")
		}
		counter++
	}
	stream.SetTrailer(grpc_metadata.Pairs(serverTrailerMdKey, "I like ending turtles."))
	return nil
}

func TestPingEmptyCarriesClientMetadata(t *testing.T) {
	ctx, client := setupProxy(t)

	ctx = grpc_metadata.NewOutgoingContext(ctx, grpc_metadata.Pairs(clientMdKey, "true"))
	out, err := client.PingEmpty(ctx, &pb.Empty{})
	require.NoError(t, err, "PingEmpty should succeed without errors")
	testhelper.ProtoEqual(t, &pb.PingResponse{Value: pingDefaultValue, Counter: 42}, out)
}

func TestPingEmpty_StressTest(t *testing.T) {
	for i := 0; i < 50; i++ {
		TestPingEmptyCarriesClientMetadata(t)
	}
}

func TestPingCarriesServerHeadersAndTrailers(t *testing.T) {
	ctx, client := setupProxy(t)

	headerMd := make(grpc_metadata.MD)
	trailerMd := make(grpc_metadata.MD)
	// This is an awkward calling convention... but meh.
	out, err := client.Ping(ctx, &pb.PingRequest{Value: "foo"}, grpc.Header(&headerMd), grpc.Trailer(&trailerMd))
	require.NoError(t, err, "Ping should succeed without errors")
	testhelper.ProtoEqual(t, &pb.PingResponse{Value: "foo", Counter: 42}, out)
	assert.Contains(t, headerMd, serverHeaderMdKey, "server response headers must contain server data")
	assert.Len(t, trailerMd, 1, "server response trailers must contain server data")
}

func TestPingErrorPropagatesAppError(t *testing.T) {
	ctx, client := setupProxy(t)

	sentryTriggered := 0
	sentrySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sentryTriggered++
	}))
	defer sentrySrv.Close()

	// minimal required sentry client configuration
	sentryURL, err := url.Parse(sentrySrv.URL)
	require.NoError(t, err)
	sentryURL.User = url.UserPassword("stub", "stub")
	sentryURL.Path = "/stub/1"

	require.NoError(t, sentry.Init(sentry.ClientOptions{
		Dsn:       sentryURL.String(),
		Transport: sentry.NewHTTPSyncTransport(),
	}))

	sentry.CaptureEvent(sentry.NewEvent())
	require.Equal(t, 1, sentryTriggered, "sentry configured incorrectly")

	_, err = client.PingError(ctx, &pb.PingRequest{Value: "foo"})
	require.Error(t, err, "PingError should never succeed")
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, "Userspace error.", status.Convert(err).Message())
	require.Equal(t, 1, sentryTriggered, "sentry must not be triggered because errors from remote must be just propagated")
}

func TestDirectorErrorIsPropagated(t *testing.T) {
	ctx, client := setupProxy(t)

	// See setupProxy where the StreamDirector has a special case.
	ctx = grpc_metadata.NewOutgoingContext(ctx, grpc_metadata.Pairs(rejectingMdKey, "true"))
	_, err := client.Ping(ctx, &pb.PingRequest{Value: "foo"})
	require.Error(t, err, "Director should reject this RPC")
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.Equal(t, "testing rejection", status.Convert(err).Message())
}

func TestPingStream_FullDuplexWorks(t *testing.T) {
	ctx, client := setupProxy(t)

	stream, err := client.PingStream(ctx)
	require.NoError(t, err, "PingStream request should be successful.")

	for i := 0; i < countListResponses; i++ {
		ping := &pb.PingRequest{Value: fmt.Sprintf("foo:%d", i)}
		require.NoError(t, stream.Send(ping), "sending to PingStream must not fail")
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if i == 0 {
			// Check that the header arrives before all entries.
			headerMd, err := stream.Header()
			require.NoError(t, err, "PingStream headers should not error.")
			assert.Contains(t, headerMd, serverHeaderMdKey, "PingStream response headers user contain metadata")
		}
		assert.EqualValues(t, i, resp.Counter, "ping roundtrip must succeed with the correct id")
	}
	require.NoError(t, stream.CloseSend(), "no error on close send")
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err, "stream should close with io.EOF, meaining OK")
	// Check that the trailer headers are here.
	trailerMd := stream.Trailer()
	assert.Len(t, trailerMd, 1, "PingList trailer headers user contain metadata")
}

func TestPingStream_StressTest(t *testing.T) {
	for i := 0; i < 50; i++ {
		TestPingStream_FullDuplexWorks(t)
	}
}

func setupProxy(t *testing.T) (context.Context, pb.TestServiceClient) {
	t.Helper()

	ctx := testhelper.Context(t)

	listenerServer := newListener(t)

	// Setup of the proxy's Director.
	proxy2Server, err := grpc.Dial(listenerServer.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, proxy2Server) })

	director := func(ctx context.Context, _ string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
		payload, err := peeker.Peek()
		if err != nil {
			return nil, err
		}

		md, ok := grpc_metadata.FromIncomingContext(ctx)
		if ok {
			if _, exists := md[rejectingMdKey]; exists {
				return proxy.NewStreamParameters(proxy.Destination{Ctx: metadata.IncomingToOutgoing(ctx), Msg: payload}, nil, nil, nil), status.Errorf(codes.PermissionDenied, "testing rejection")
			}
		}

		// Explicitly copy the metadata, otherwise the tests will fail.
		return proxy.NewStreamParameters(proxy.Destination{
			Ctx:  metadata.IncomingToOutgoing(ctx),
			Conn: proxy2Server,
			Msg:  payload,
		}, nil, nil, nil), nil
	}

	// Setup backend server for test suite
	backendServer := grpc.NewServer()
	pb.RegisterTestServiceServer(backendServer, &assertingService{t: t})
	go func() {
		backendServer.Serve(listenerServer)
	}()
	t.Cleanup(backendServer.Stop)

	client2Proxy := newProxy(t, ctx, director, "mwitkow.testproto.TestService", "Ping")

	return ctx, pb.NewTestServiceClient(client2Proxy)
}

func TestProxyErrorPropagation(t *testing.T) {
	errBackend := status.Error(codes.InvalidArgument, "backend error")
	errDirector := status.Error(codes.FailedPrecondition, "director error")
	errRequestFinalizer := status.Error(codes.Internal, "request finalizer error")

	for _, tc := range []struct {
		desc                  string
		backendError          error
		directorError         error
		requestFinalizerError error
		returnedError         error
		errHandler            func(error) error
	}{
		{
			desc:          "backend error is propagated",
			backendError:  errBackend,
			returnedError: errBackend,
		},
		{
			desc:          "director error is propagated",
			directorError: errDirector,
			returnedError: errDirector,
		},
		{
			desc:                  "request finalizer error is propagated",
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errRequestFinalizer,
		},
		{
			desc:                  "director error cancels proxying",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			directorError:         errDirector,
			returnedError:         errDirector,
		},
		{
			desc:                  "backend error prioritized over request finalizer error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errBackend,
		},
		{
			desc:                  "err handler gets error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errBackend,
			errHandler: func(err error) error {
				testhelper.RequireGrpcError(t, errBackend, err)
				return errBackend
			},
		},
		{
			desc:          "err handler can swallow error",
			backendError:  errBackend,
			returnedError: io.EOF,
			errHandler: func(err error) error {
				testhelper.RequireGrpcError(t, errBackend, err)
				return nil
			},
		},
		{
			desc:                  "swallowed error surfaces request finalizer error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errRequestFinalizer,
			errHandler: func(err error) error {
				testhelper.RequireGrpcError(t, errBackend, err)
				return nil
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tmpDir := testhelper.TempDir(t)

			backendListener, err := net.Listen("unix", filepath.Join(tmpDir, "backend"))
			require.NoError(t, err)

			backendServer := grpc.NewServer(grpc.UnknownServiceHandler(func(interface{}, grpc.ServerStream) error {
				return tc.backendError
			}))
			go func() { backendServer.Serve(backendListener) }()
			defer backendServer.Stop()
			ctx := testhelper.Context(t)

			backendClientConn, err := grpc.DialContext(ctx, "unix://"+backendListener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
			require.NoError(t, err)
			defer func() {
				require.NoError(t, backendClientConn.Close())
			}()

			proxyListener, err := net.Listen("unix", filepath.Join(tmpDir, "proxy"))
			require.NoError(t, err)

			proxyServer := grpc.NewServer(
				grpc.ForceServerCodec(proxy.NewCodec()),
				grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
					return proxy.NewStreamParameters(
						proxy.Destination{
							Ctx:        ctx,
							Conn:       backendClientConn,
							ErrHandler: tc.errHandler,
						},
						nil,
						func() error { return tc.requestFinalizerError },
						nil,
					), tc.directorError
				})),
			)

			go func() { proxyServer.Serve(proxyListener) }()
			defer proxyServer.Stop()

			proxyClientConn, err := grpc.DialContext(ctx, "unix://"+proxyListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer func() {
				require.NoError(t, proxyClientConn.Close())
			}()

			resp, err := pb.NewTestServiceClient(proxyClientConn).Ping(ctx, &pb.PingRequest{})
			testhelper.RequireGrpcError(t, tc.returnedError, err)
			require.Nil(t, resp)
		})
	}
}

func TestRegisterStreamHandlers(t *testing.T) {
	directorCalledError := errors.New("director was called")

	server := grpc.NewServer(
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
			return nil, directorCalledError
		})),
	)

	var pingStreamHandlerCalled, pingEmptyStreamHandlerCalled bool

	pingValue := "hello"

	pingStreamHandler := func(srv interface{}, stream grpc.ServerStream) error {
		pingStreamHandlerCalled = true
		var req pb.PingRequest

		if err := stream.RecvMsg(&req); err != nil {
			return err
		}

		require.Equal(t, pingValue, req.Value)

		return stream.SendMsg(nil)
	}

	pingEmptyStreamHandler := func(srv interface{}, stream grpc.ServerStream) error {
		pingEmptyStreamHandlerCalled = true
		var req pb.Empty

		if err := stream.RecvMsg(&req); err != nil {
			return err
		}

		return stream.SendMsg(nil)
	}

	streamers := map[string]grpc.StreamHandler{
		"Ping":      pingStreamHandler,
		"PingEmpty": pingEmptyStreamHandler,
	}

	proxy.RegisterStreamHandlers(server, "mwitkow.testproto.TestService", streamers)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)
	defer server.Stop()

	cc, err := client.Dial("unix://"+serverSocketPath, []grpc.DialOption{grpc.WithBlock()})
	require.NoError(t, err)
	defer cc.Close()

	testServiceClient := pb.NewTestServiceClient(cc)
	ctx := testhelper.Context(t)

	_, err = testServiceClient.Ping(ctx, &pb.PingRequest{Value: pingValue})
	require.NoError(t, err)
	require.True(t, pingStreamHandlerCalled)

	_, err = testServiceClient.PingEmpty(ctx, &pb.Empty{})
	require.NoError(t, err)
	require.True(t, pingEmptyStreamHandlerCalled)

	// since PingError was never registered with its own streamer, it should get sent to the UnknownServiceHandler
	_, err = testServiceClient.PingError(ctx, &pb.PingRequest{})
	testhelper.RequireGrpcError(t, status.Error(codes.Unknown, directorCalledError.Error()), err)
}
