package limithandler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ErrMaxQueueTime indicates a request has reached the maximum time allowed to wait in the
// concurrency queue.
var ErrMaxQueueTime = errors.New("maximum time in concurrency queue reached")

// ErrMaxQueueSize indicates the concurrency queue has reached its maximum size
var ErrMaxQueueSize = errors.New("maximum queue size reached")

// QueueTickerCreator is a function that provides a ticker
type QueueTickerCreator func() helper.Ticker

// keyedConcurrencyLimiter is a concurrency limiter that applies to a specific keyed resource.
type keyedConcurrencyLimiter struct {
	// tokens is the channel of available tokens, where every token allows one concurrent call
	// to the concurrency-limited function.
	tokens                 chan struct{}
	refcount               int
	maxQueuedTickerCreator QueueTickerCreator
}

// acquire tries to acquire the semaphore. It may fail if there is a max queue-time ticker that
// ticks before acquiring a token.
func (sem *keyedConcurrencyLimiter) acquire(ctx context.Context) error {
	var ticker helper.Ticker

	if sem.maxQueuedTickerCreator != nil {
		ticker = sem.maxQueuedTickerCreator()
	} else {
		ticker = helper.Ticker(helper.NewManualTicker())
	}

	defer ticker.Stop()
	ticker.Reset()

	select {
	case sem.tokens <- struct{}{}:
		return nil
	case <-ticker.C():
		return ErrMaxQueueTime
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release releases the semaphore.
func (sem *keyedConcurrencyLimiter) release() { <-sem.tokens }

// ConcurrencyLimiter contains rate limiter state
type ConcurrencyLimiter struct {
	// maxConcurrencyLimit is the maximum number of concurrent calls to the limited function.
	// This limit is per key.
	maxConcurrencyLimit int64
	// maxQueueLength is the maximum number of operations allowed to wait in a queued state.
	// This limit is global and applies before the concurrency limit. Subsequent incoming
	// operations will be rejected with an error immediately.
	maxQueueLength int64
	// maxQueuedTickerCreator is a function that creates a ticker used to determine how long a
	// call may be queued.
	maxQueuedTickerCreator QueueTickerCreator

	// monitor is a monitor that will get notified of the state of concurrency-limited RPC
	// calls.
	monitor ConcurrencyMonitor

	m sync.RWMutex
	// queued tracks the current number of operations waiting in the admission queue.
	queued int64
	// limitsByKey tracks all concurrency limits per key. Its per-key entries are lazily created
	// and will get evicted once there are no concurrency-limited calls for any such key
	// anymore.
	limitsByKey map[string]*keyedConcurrencyLimiter
}

// NewConcurrencyLimiter creates a new concurrency rate limiter.
func NewConcurrencyLimiter(maxConcurrencyLimit, maxQueueLength int, maxQueuedTickerCreator QueueTickerCreator, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = NewNoopConcurrencyMonitor()
	}

	return &ConcurrencyLimiter{
		maxConcurrencyLimit:    int64(maxConcurrencyLimit),
		maxQueueLength:         int64(maxQueueLength),
		maxQueuedTickerCreator: maxQueuedTickerCreator,
		monitor:                monitor,
		limitsByKey:            make(map[string]*keyedConcurrencyLimiter),
	}
}

// Limit will limit the concurrency of the limited function f. There are two distinct mechanisms
// that limit execution of the function:
//
//  1. First, every call will enter the queue. This queue is global across all keys and limits how
//     many callers may try to acquire their per-key semaphore at the same time. If the queue is full
//     the caller will be rejected.
//  2. Second, when the caller has successfully entered the queue, they try to acquire their per-key
//     semaphore. If this takes longer than the maximum queueing limit then the caller will be
//     dequeued and gets an error.
func (c *ConcurrencyLimiter) Limit(ctx context.Context, limitingKey string, f LimitedFunc) (interface{}, error) {
	if c.maxConcurrencyLimit <= 0 {
		return f()
	}

	var decremented bool

	log := ctxlogrus.Extract(ctx).WithField("limiting_key", limitingKey)
	if err := c.queueInc(ctx); err != nil {
		if errors.Is(err, ErrMaxQueueSize) {
			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueSize).WithDetail(
				&gitalypb.LimitError{
					ErrorMessage: err.Error(),
					RetryAfter:   durationpb.New(0),
				},
			)
		}

		log.WithError(err).Error("unexpected error when queueing request")
		return nil, err
	}
	defer c.queueDec(&decremented)

	start := time.Now()
	c.monitor.Queued(ctx)

	sem := c.getConcurrencyLimit(limitingKey)
	defer c.putConcurrencyLimit(limitingKey)

	err := sem.acquire(ctx)
	c.queueDec(&decremented)

	c.monitor.Dequeued(ctx)
	if err != nil {
		if errors.Is(err, ErrMaxQueueTime) {
			c.monitor.Dropped(ctx, "max_time")

			return nil, structerr.NewResourceExhausted("%w", ErrMaxQueueTime).WithDetail(&gitalypb.LimitError{
				ErrorMessage: err.Error(),
				RetryAfter:   durationpb.New(0),
			})
		}

		log.WithError(err).Error("unexpected error when dequeueing request")
		return nil, err
	}
	defer sem.release()

	c.monitor.Enter(ctx, time.Since(start))
	defer c.monitor.Exit(ctx)

	return f()
}

// getConcurrencyLimit retrieves the concurrency limit for the given key. If no such limiter exists
// it will be lazily constructed.
func (c *ConcurrencyLimiter) getConcurrencyLimit(limitingKey string) *keyedConcurrencyLimiter {
	c.m.Lock()
	defer c.m.Unlock()

	if c.limitsByKey[limitingKey] == nil {
		c.limitsByKey[limitingKey] = &keyedConcurrencyLimiter{
			tokens:                 make(chan struct{}, c.maxConcurrencyLimit),
			maxQueuedTickerCreator: c.maxQueuedTickerCreator,
		}
	}

	c.limitsByKey[limitingKey].refcount++
	return c.limitsByKey[limitingKey]
}

// putConcurrencyLimit drops the reference to the concurrency limit identified by the given key.
// This must only ever be called after `getConcurrencyLimit()` for the same key. If the reference
// count of the concurrency limit drops to zero then it will be destroyed.
func (c *ConcurrencyLimiter) putConcurrencyLimit(limitingKey string) {
	c.m.Lock()
	defer c.m.Unlock()

	ref := c.limitsByKey[limitingKey]
	if ref == nil {
		panic("semaphore should be in the map")
	}

	if ref.refcount <= 0 {
		panic(fmt.Sprintf("bad semaphore ref refcount %d", ref.refcount))
	}

	ref.refcount--
	if ref.refcount == 0 {
		delete(c.limitsByKey, limitingKey)
	}
}

func (c *ConcurrencyLimiter) countSemaphores() int {
	c.m.RLock()
	defer c.m.RUnlock()

	return len(c.limitsByKey)
}

func (c *ConcurrencyLimiter) queueInc(ctx context.Context) error {
	c.m.Lock()
	defer c.m.Unlock()

	if c.maxQueueLength > 0 &&
		c.queued >= c.maxQueueLength {
		c.monitor.Dropped(ctx, "max_size")
		return ErrMaxQueueSize
	}

	c.queued++
	return nil
}

func (c *ConcurrencyLimiter) queueDec(decremented *bool) {
	if decremented == nil || *decremented {
		return
	}
	*decremented = true
	c.m.Lock()
	defer c.m.Unlock()

	c.queued--
}

// WithConcurrencyLimiters sets up middleware to limit the concurrency of
// requests based on RPC and repository
func WithConcurrencyLimiters(cfg config.Cfg, middleware *LimiterMiddleware) {
	acquiringSecondsMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "acquiring_seconds",
			Help:      "Histogram of time calls are rate limited (in seconds)",
			Buckets:   cfg.Prometheus.GRPCLatencyBuckets,
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	inProgressMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "in_progress",
			Help:      "Gauge of number of concurrent in-progress calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	queuedMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "concurrency_limiting",
			Name:      "queued",
			Help:      "Gauge of number of queued calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)

	middleware.collect = func(metrics chan<- prometheus.Metric) {
		acquiringSecondsMetric.Collect(metrics)
		inProgressMetric.Collect(metrics)
		queuedMetric.Collect(metrics)
	}

	result := make(map[string]Limiter)

	newTickerFunc := func() helper.Ticker {
		return helper.NewManualTicker()
	}

	for _, limit := range cfg.Concurrency {
		if limit.MaxQueueWait > 0 {
			limit := limit
			newTickerFunc = func() helper.Ticker {
				return helper.NewTimerTicker(limit.MaxQueueWait.Duration())
			}
		}

		result[limit.RPC] = NewConcurrencyLimiter(
			limit.MaxPerRepo,
			limit.MaxQueueSize,
			newTickerFunc,
			newPerRPCPromMonitor("gitaly", limit.RPC, queuedMetric, inProgressMetric,
				acquiringSecondsMetric, middleware.requestsDroppedMetric),
		)
	}

	// Set default for ReplicateRepository.
	replicateRepositoryFullMethod := "/gitaly.RepositoryService/ReplicateRepository"
	if _, ok := result[replicateRepositoryFullMethod]; !ok {
		result[replicateRepositoryFullMethod] = NewConcurrencyLimiter(
			1,
			0,
			func() helper.Ticker {
				return helper.NewManualTicker()
			},
			newPerRPCPromMonitor("gitaly", replicateRepositoryFullMethod, queuedMetric,
				inProgressMetric, acquiringSecondsMetric, middleware.requestsDroppedMetric))
	}

	middleware.methodLimiters = result
}
