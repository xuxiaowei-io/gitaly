package praefect

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/proxy"
	"google.golang.org/grpc"
)

// ReplicateRepositoryHandler intercepts ReplicateRepository RPC calls.
func ReplicateRepositoryHandler(coordinator *Coordinator) grpc.StreamHandler {
	return func(srv interface{}, stream grpc.ServerStream) error {
		// Fallback to the default transparent handler to proxy the RPC.
		return proxy.TransparentHandler(coordinator.StreamDirector)(srv, stream)
	}
}
