package limithandler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"golang.org/x/time/rate"
)

// RateLimiter is an implementation of Limiter that puts a hard limit on the
// number of requests per second
type RateLimiter struct {
	limitersByKey         sync.Map
	refillInterval        time.Duration
	burst                 int
	requestsDroppedMetric prometheus.Counter
}

// Limit rejects an incoming reequest if the maximum number of requests per
// second has been reached
func (r *RateLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	limiter, _ := r.limitersByKey.LoadOrStore(
		lockKey,
		rate.NewLimiter(rate.Every(r.refillInterval), r.burst),
	)
	if !limiter.(*rate.Limiter).Allow() {
		// For now, we are only emitting this metric to get an idea of the shape
		// of traffic.
		r.requestsDroppedMetric.Inc()
		if featureflag.RateLimit.IsEnabled(ctx) {
			return nil, helper.ErrUnavailable(errors.New("too many requests"))
		}
	}

	return f()
}

// NewRateLimiter creates a new instance of RateLimiter
func NewRateLimiter(
	refillInterval time.Duration,
	burst int,
	requestsDroppedMetric prometheus.Counter,
) *RateLimiter {
	r := &RateLimiter{
		refillInterval:        refillInterval,
		burst:                 burst,
		requestsDroppedMetric: requestsDroppedMetric,
	}

	return r
}

// WithRateLimiters sets up a middleware with limiters that limit requests
// based on its rate per second per RPC
func WithRateLimiters(cfg config.Cfg, middleware *LimiterMiddleware) {
	result := make(map[string]Limiter)

	for _, limitCfg := range cfg.RateLimiting {
		if limitCfg.Burst > 0 && limitCfg.Interval > 0 {
			serviceName, methodName := splitMethodName(limitCfg.RPC)
			result[limitCfg.RPC] = NewRateLimiter(
				limitCfg.Interval,
				limitCfg.Burst,
				middleware.requestsDroppedMetric.With(prometheus.Labels{
					"system":       "gitaly",
					"grpc_service": serviceName,
					"grpc_method":  methodName,
					"reason":       "rate",
				}),
			)
		}
	}

	middleware.methodLimiters = result
}
