package limithandler

import (
	"bytes"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	promconfig "gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
)

func TestNewPerRPCPromMonitor(t *testing.T) {
	system := "gitaly"
	fullMethod := "fullMethod"
	acquiringSecondsVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "acquiring_seconds",
			Help:    "seconds to acquire",
			Buckets: promconfig.DefaultConfig().GRPCLatencyBuckets,
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	inProgressMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "in_progress",
			Help: "requests in progress",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	queuedMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "queued",
			Help: "number of queued requests",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
	requestsDroppedMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dropped",
			Help: "number of dropped requests",
		},
		[]string{
			"system",
			"grpc_service",
			"grpc_method",
			"reason",
		},
	)

	rpcMonitor := newPerRPCPromMonitor(
		system,
		fullMethod,
		queuedMetric,
		inProgressMetric,
		acquiringSecondsVec,
		requestsDroppedMetric,
	)

	ctx := testhelper.Context(t)

	rpcMonitor.Queued(ctx)
	rpcMonitor.Enter(ctx, time.Second)
	rpcMonitor.Dropped(ctx, "load")

	expectedMetrics := `# HELP acquiring_seconds seconds to acquire
# TYPE acquiring_seconds histogram
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.001"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.005"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.025"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.1"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="0.5"} 0
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="10"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="30"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="60"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="300"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="1500"} 1
acquiring_seconds_bucket{grpc_method="unknown",grpc_service="unknown",system="gitaly",le="+Inf"} 1
acquiring_seconds_sum{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
acquiring_seconds_count{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
# HELP dropped number of dropped requests
# TYPE dropped counter
dropped{grpc_method="unknown",grpc_service="unknown",reason="load",system="gitaly"} 1
# HELP in_progress requests in progress
# TYPE in_progress gauge
in_progress{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1
# HELP queued number of queued requests
# TYPE queued gauge
queued{grpc_method="unknown",grpc_service="unknown",system="gitaly"} 1

`
	require.NoError(t, testutil.CollectAndCompare(
		rpcMonitor,
		bytes.NewBufferString(expectedMetrics),
		"in_progress",
		"queued",
		"dropped",
		"acquiring_seconds",
	))
}

func TestNewPackObjectsConcurrencyMonitor(t *testing.T) {
	ctx := testhelper.Context(t)

	m := NewPackObjectsConcurrencyMonitor(
		"user",
		promconfig.DefaultConfig().GRPCLatencyBuckets,
	)

	m.Queued(ctx)
	m.Enter(ctx, time.Second)
	m.Dropped(ctx, "load")

	expectedMetrics := `# HELP gitaly_pack_objects_acquiring_seconds Histogram of time calls are rate limited (in seconds)
# TYPE gitaly_pack_objects_acquiring_seconds histogram
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="0.001"} 0
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="0.005"} 0
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="0.025"} 0
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="0.1"} 0
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="0.5"} 0
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="1"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="10"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="30"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="60"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="300"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="1500"} 1
gitaly_pack_objects_acquiring_seconds_bucket{type="user",le="+Inf"} 1
gitaly_pack_objects_acquiring_seconds_sum{type="user"} 1
gitaly_pack_objects_acquiring_seconds_count{type="user"} 1
# HELP gitaly_pack_objects_dropped_total Number of requests dropped from the queue
# TYPE gitaly_pack_objects_dropped_total counter
gitaly_pack_objects_dropped_total{reason="load",type="user"} 1
# HELP gitaly_pack_objects_queued Gauge of number of queued calls
# TYPE gitaly_pack_objects_queued gauge
gitaly_pack_objects_queued{type="user"} 1

`
	require.NoError(t, testutil.CollectAndCompare(
		m,
		bytes.NewBufferString(expectedMetrics),
		"gitaly_pack_objects_acquiring_seconds",
		"gitaly_pack_objecfts_in_progress",
		"gitaly_pack_objects_queued",
		"gitaly_pack_objects_dropped_total",
	))
}
