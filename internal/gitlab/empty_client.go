package gitlab

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
)

// EmptyFeatureGetter is a noop feature getter
type EmptyFeatureGetter struct{}

// Features returns an empty map
func (e *EmptyFeatureGetter) Features(ctx context.Context) (map[featureflag.FeatureFlag]bool, error) {
	return map[featureflag.FeatureFlag]bool{}, nil
}

// Describe does nothing
func (e *EmptyFeatureGetter) Describe(_ chan<- *prometheus.Desc) {}

// Collect does nothing
func (e *EmptyFeatureGetter) Collect(_ chan<- prometheus.Metric) {}
