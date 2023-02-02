package limithandler

import (
	"context"
	"sync/atomic"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type limitStatsKey struct{}

// LimitStats contains info about the concurrency limiter.
type LimitStats struct {
	// concurrencyQueueMs milliseconds waiting in concurrency limit queue.
	concurrencyQueueMs int64
}

// InitLimitStats initializes context with a per-RPC stats struct
func InitLimitStats(ctx context.Context) context.Context {
	return context.WithValue(ctx, limitStatsKey{}, &LimitStats{})
}

// AddConcurrencyQueueMs adds queue time.
func (s *LimitStats) AddConcurrencyQueueMs(queueMs int64) {
	atomic.AddInt64(&s.concurrencyQueueMs, queueMs)
}

// Fields returns logging info.
func (s *LimitStats) Fields() logrus.Fields {
	return logrus.Fields{
		"limit.concurrency_queue_ms": s.concurrencyQueueMs,
	}
}

// FieldsProducer extracts stats info from the context and returns it as a logging fields.
func FieldsProducer(ctx context.Context, _ error) logrus.Fields {
	stats := limitStatsFromContext(ctx)
	if stats != nil {
		return stats.Fields()
	}
	return nil
}

func limitStatsFromContext(ctx context.Context) *LimitStats {
	v, ok := ctx.Value(limitStatsKey{}).(*LimitStats)
	if !ok {
		return nil
	}
	return v
}

// stats interceptors are separate from middleware and serve one main purpose:
// initialize the stats object early, so that logrus can see it.
// it must be placed before the limithandler middleware

// StatsUnaryInterceptor returns a Unary Interceptor that initializes the context
func StatsUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = InitLimitStats(ctx)

	res, err := handler(ctx, req)

	return res, err
}

// StatsStreamInterceptor returns a Stream Interceptor
func StatsStreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	ctx = InitLimitStats(ctx)

	wrapped := grpcmw.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	return err
}
