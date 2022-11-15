package featureflag

import (
	"context"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
)

// FieldsProducer adds feature_flags logging fields to gRPC logs. Only enabled flags are available.
// The log field looks something like following.
//
//	{
//	   "feature_flags": "feature_a feature_b feature_c"
//	}
func FieldsProducer(ctx context.Context, err error) logrus.Fields {
	// By default, we log feature flags for gRPC failures only. This is due to the potential
	// huge logging volume. The log aggregator may have a hard time processing them. As a
	// result, we only put the logs in failed RPCs only.
	if err == nil && featureflag.AlwaysLogFeatureFlags.IsDisabled(ctx) {
		return nil
	}

	var enabledFlags []string
	for flag, value := range featureflag.FromContext(ctx) {
		if value {
			enabledFlags = append(enabledFlags, flag.Name)
		}
	}
	if len(enabledFlags) == 0 {
		return nil
	}

	sort.Strings(enabledFlags)
	return logrus.Fields{
		"feature_flags": strings.Join(enabledFlags, " "),
	}
}
