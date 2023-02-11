package structerr

import (
	"context"
	"errors"
	"sort"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// UnaryInterceptor is an interceptor for unary RPC calls that injects error metadata as detailed
// error. This is only supposed to be used for testing purposes as error metadata is considered to
// be a server-side detail. No clients should start to rely on it.
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	response, err := handler(ctx, req)
	return response, interceptedError(err)
}

// StreamInterceptor is an interceptor for streaming RPC calls that injects error metadata as
// detailed error. This is only supposed to be used for testing purposes as error metadata is
// considered to be a server-side detail. No clients should start to rely on it.
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return interceptedError(handler(srv, stream))
}

func interceptedError(err error) error {
	var structErr Error
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
		errWithMetadata := New("%w", err)
		for _, key := range keys {
			errWithMetadata = errWithMetadata.WithInterceptedMetadata(key, metadata[key])
		}

		return errWithMetadata
	}

	return err
}

// FieldsProducer extracts metadata from err if it contains a `structerr.Error` and exposes it as
// logged fields. This function is supposed to be used with `log.MessageProducer()`.
func FieldsProducer(_ context.Context, err error) logrus.Fields {
	var structErr Error
	if errors.As(err, &structErr) {
		metadata := structErr.Metadata()
		if len(metadata) == 0 {
			return nil
		}

		return logrus.Fields{
			"error_metadata": metadata,
		}
	}

	return nil
}
