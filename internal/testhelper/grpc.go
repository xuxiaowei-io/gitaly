package testhelper

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SetCtxGrpcMethod will set the gRPC context value for the proper key
// responsible for an RPC full method name. This directly corresponds to the
// gRPC function responsible for extracting the method:
// https://godoc.org/google.golang.org/grpc#Method
func SetCtxGrpcMethod(ctx context.Context, method string) context.Context {
	return grpc.NewContextWithServerTransportStream(ctx, mockServerTransportStream{method})
}

type mockServerTransportStream struct {
	method string
}

func (msts mockServerTransportStream) Method() string             { return msts.method }
func (mockServerTransportStream) SetHeader(md metadata.MD) error  { return nil }
func (mockServerTransportStream) SendHeader(md metadata.MD) error { return nil }
func (mockServerTransportStream) SetTrailer(md metadata.MD) error { return nil }

// RequireGrpcError asserts the passed err is of the same code as expectedCode.
func RequireGrpcError(t testing.TB, err error, expectedCode codes.Code) {
	t.Helper()

	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Check that the code matches
	status, _ := status.FromError(err)
	if code := status.Code(); code != expectedCode {
		t.Fatalf("Expected an error with code %v, got %v. The error was %q", expectedCode, code, err.Error())
	}
}

// GrpcErrorHasMessage checks whether the GRPC error's message matches the
// given message.
func GrpcErrorHasMessage(t testing.TB, grpcError error, msg string) {
	t.Helper()

	st, ok := status.FromError(grpcError)
	require.Truef(t, ok, "passed err is not a status.Status: %T", grpcError)
	require.Equal(t, msg, st.Message())
}

// MergeOutgoingMetadata merges provided metadata-s and returns context with resulting value.
func MergeOutgoingMetadata(ctx context.Context, md ...metadata.MD) context.Context {
	ctxmd, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.NewOutgoingContext(ctx, metadata.Join(md...))
	}

	return metadata.NewOutgoingContext(ctx, metadata.Join(append(md, ctxmd)...))
}

// MergeIncomingMetadata merges provided metadata-s and returns context with resulting value.
func MergeIncomingMetadata(ctx context.Context, md ...metadata.MD) context.Context {
	ctxmd, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metadata.NewIncomingContext(ctx, metadata.Join(md...))
	}

	return metadata.NewIncomingContext(ctx, metadata.Join(append(md, ctxmd)...))
}

// ProtoEqual asserts that expected and actual protobuf messages are equal.
// This is required as comparing messages directly with `require.Equal` doesn't
// work.
func ProtoEqual(t testing.TB, expected proto.Message, actual proto.Message) {
	require.True(t, proto.Equal(expected, actual), "proto messages not equal\nexpected: %v\ngot:      %v", expected, actual)
}
