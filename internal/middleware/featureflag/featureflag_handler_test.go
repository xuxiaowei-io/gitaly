//go:build !gitaly_test_sha256

package featureflag

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
)

// This test doesn't use testhelper.NewFeatureSets intentionally.
func TestFeatureFlagLogs(t *testing.T) {
	t.Parallel()

	featureA := featureflag.FeatureFlag{Name: "feature_a"}
	featureB := featureflag.FeatureFlag{Name: "feature_b"}
	featureC := featureflag.FeatureFlag{Name: "feature_c"}
	testCases := []struct {
		desc           string
		featureFlags   map[featureflag.FeatureFlag]bool
		expectedErr    error
		expectedFields logrus.Fields
	}{
		{
			desc: "some feature flags are enabled in successful RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: true,
				featureB: false,
				featureC: true,
			},
			expectedFields: logrus.Fields{"feature_flags": "feature_a feature_c"},
		}, {
			desc: "some feature flags are enabled in failed RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: true,
				featureB: false,
				featureC: true,
			},
			expectedErr:    helper.ErrInternalf("something goes wrong"),
			expectedFields: logrus.Fields{"feature_flags": "feature_a feature_c"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			for flag, value := range tc.featureFlags {
				ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, flag, value)
			}
			fields := FieldsProducer(ctx, tc.expectedErr)
			require.Equal(t, tc.expectedFields, fields)
		})
	}
}
