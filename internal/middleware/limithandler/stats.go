package limithandler

import (
	"context"
	"sync"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type limitStatsKey struct{}

// LimitStats contains info about the concurrency limiter.
type LimitStats struct {
	sync.Mutex
	// limitingKey is the key used for limiting accounting
	limitingKey string
	// concurrencyQueueLen is the combination of in-flight requests and in-queue requests. It tells
	// how busy the queue of the same limiting key is
	concurrencyQueueLength int
	// concurrencyQueueMs milliseconds waiting in concurrency limit queue.
	concurrencyQueueMs int64
	// concurrencyDropped stores the dropping reason of a request
	concurrencyDropped string
}

// InitLimitStats initializes context with a per-RPC stats struct.
func InitLimitStats(ctx context.Context) context.Context {
	return context.WithValue(ctx, limitStatsKey{}, &LimitStats{})
}

// AddConcurrencyQueueMs adds queue time.
func (s *LimitStats) AddConcurrencyQueueMs(queueMs int64) {
	s.Lock()
	defer s.Unlock()
	s.concurrencyQueueMs = queueMs
}

// SetLimitingKey set limiting key.
func (s *LimitStats) SetLimitingKey(limitingKey string) {
	s.Lock()
	defer s.Unlock()
	s.limitingKey = limitingKey
}

// SetConcurrencyQueueLength set concurrency queue length.
func (s *LimitStats) SetConcurrencyQueueLength(queueLength int) {
	s.Lock()
	defer s.Unlock()
	s.concurrencyQueueLength = queueLength
}

// SetConcurrencyDroppedReason sets the reason why a call has been dropped from the queue.
func (s *LimitStats) SetConcurrencyDroppedReason(reason string) {
	s.Lock()
	defer s.Unlock()
	s.concurrencyDropped = reason
}

// Fields returns logging info.
func (s *LimitStats) Fields() logrus.Fields {
	s.Lock()
	defer s.Unlock()

	if s.limitingKey == "" {
		return nil
	}
	logs := logrus.Fields{
		"limit.limiting_key":             s.limitingKey,
		"limit.concurrency_queue_ms":     s.concurrencyQueueMs,
		"limit.concurrency_queue_length": s.concurrencyQueueLength,
	}
	if s.concurrencyDropped != "" {
		logs["limit.concurrency_dropped"] = s.concurrencyDropped
	}
	return logs
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
// it must be placed before the limithandler middleware.

// StatsUnaryInterceptor returns a Unary Interceptor that initializes the context.
func StatsUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = InitLimitStats(ctx)

	res, err := handler(ctx, req)

	return res, err
}

// StatsStreamInterceptor returns a Stream Interceptor.
func StatsStreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	ctx = InitLimitStats(ctx)

	wrapped := grpcmw.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	return err
}
