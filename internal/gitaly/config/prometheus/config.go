package prometheus

import (
	"fmt"
	"sort"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/errors/cfgerror"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/duration"
)

// Config contains additional configuration data for prometheus
type Config struct {
	// ScrapeTimeout is the allowed duration of a Prometheus scrape before timing out.
	ScrapeTimeout duration.Duration `toml:"scrape_timeout,omitempty"`
	// GRPCLatencyBuckets configures the histogram buckets used for gRPC
	// latency measurements.
	GRPCLatencyBuckets []float64 `toml:"grpc_latency_buckets,omitempty"`
}

// DefaultConfig returns a new config with default values set.
func DefaultConfig() Config {
	return Config{
		ScrapeTimeout:      duration.Duration(10 * time.Second),
		GRPCLatencyBuckets: []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1.0, 10.0, 30.0, 60.0, 300.0, 1500.0},
	}
}

// Configure configures latency buckets for prometheus timing histograms
func (c *Config) Configure() {
	if len(c.GRPCLatencyBuckets) == 0 {
		return
	}

	log.WithField("latencies", c.GRPCLatencyBuckets).Info("grpc prometheus histograms enabled")

	grpcprometheus.EnableHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})
	grpcprometheus.EnableClientHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})
}

// Validate runs validation on all fields and compose all found errors.
func (c *Config) Validate() error {
	if len(c.GRPCLatencyBuckets) == 0 {
		return nil
	}

	errs := cfgerror.New().Append(cfgerror.IsPositive(c.ScrapeTimeout.Duration()), "scrape_timeout")
	if !sort.IsSorted(sort.Float64Slice(c.GRPCLatencyBuckets)) {
		err := fmt.Errorf("%w: expected asc: %v", cfgerror.ErrBadOrder, c.GRPCLatencyBuckets)
		errs = errs.Append(err, "grpc_latency_buckets")
	}

	return errs.AsError()
}
