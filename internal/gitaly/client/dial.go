package client

import (
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
)

// StreamInterceptor returns the stream interceptors that should be configured for a client.
func StreamInterceptor() grpc.DialOption {
	return grpc.WithChainStreamInterceptor(
		grpctracing.StreamClientTracingInterceptor(),
		grpccorrelation.StreamClientCorrelationInterceptor(),
	)
}

// UnaryInterceptor returns the unary interceptors that should be configured for a client.
func UnaryInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		grpctracing.UnaryClientTracingInterceptor(),
		grpccorrelation.UnaryClientCorrelationInterceptor(),
	)
}
