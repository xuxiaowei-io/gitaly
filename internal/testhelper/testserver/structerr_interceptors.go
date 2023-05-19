package testserver

import (
	"context"
	"errors"
	"sort"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc"
)

// StructErrUnaryInterceptor is an interceptor for unary RPC calls that injects error metadata as detailed
// error. This is only supposed to be used for testing purposes as error metadata is considered to
// be a server-side detail. No clients should start to rely on it.
func StructErrUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	response, err := handler(ctx, req)
	return response, interceptedError(err)
}

// StructErrStreamInterceptor is an interceptor for streaming RPC calls that injects error metadata as
// detailed error. This is only supposed to be used for testing purposes as error metadata is
// considered to be a server-side detail. No clients should start to rely on it.
func StructErrStreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return interceptedError(handler(srv, stream))
}

func interceptedError(err error) error {
	var structErr structerr.Error
	if errors.As(err, &structErr) {
		metadata := structErr.Metadata()
		if len(metadata) == 0 {
			return err
		}

		keys := make([]string, 0, len(metadata))
		for key := range metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		// We need to wrap the top-level error with the expected intercepted metadata, or
		// otherwise we lose error context.
		errWithMetadata := structerr.New("%w", err)
		for _, key := range keys {
			errWithMetadata = testhelper.WithInterceptedMetadata(errWithMetadata, key, metadata[key])
		}

		return errWithMetadata
	}

	return err
}
