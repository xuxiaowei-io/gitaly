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
func TestFeatureFlagLogsWithoutAlwaysLogFeatureFlags(t *testing.T) {
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
			desc:           "empty feature flags in successful RPC",
			expectedFields: nil,
		},
		{
			desc:           "empty feature flags in failed RPC",
			expectedErr:    helper.ErrInternalf("something goes wrong"),
			expectedFields: nil,
		},
		{
			desc: "all feature flags are disabled in successful RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: false,
				featureB: false,
				featureC: false,
			},
			expectedFields: nil,
		},
		{
			desc: "all feature flags are disabled in failed RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: false,
				featureB: false,
				featureC: false,
			},
			expectedErr:    helper.ErrInternalf("something goes wrong"),
			expectedFields: nil,
		},
		{
			desc: "some feature flags are enabled in successful RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: true,
				featureB: false,
				featureC: true,
			},
			expectedFields: nil, // Not log flags for successful RPC by default
		},
		{
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
			fields := FieldsProducer(featureFlagContext(tc.featureFlags, false), tc.expectedErr)
			require.Equal(t, tc.expectedFields, fields)
		})
	}
}

// This test doesn't use testhelper.NewFeatureSets intentionally.
func TestFeatureFlagLogsWithAlwaysLogFeatureFlags(t *testing.T) {
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
			desc: "some feature flags, including AlwaysLogFeatureFlags, are enabled in successful RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: true,
				featureB: false,
				featureC: true,
			},
			expectedFields: logrus.Fields{"feature_flags": "always_log_feature_flags feature_a feature_c"},
		},
		{
			desc: "some feature flags, including AlwaysLogFeatureFlags, are enabled in failed RPC",
			featureFlags: map[featureflag.FeatureFlag]bool{
				featureA: true,
				featureB: false,
				featureC: true,
			},
			expectedErr:    helper.ErrInternalf("something goes wrong"),
			expectedFields: logrus.Fields{"feature_flags": "always_log_feature_flags feature_a feature_c"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			fields := FieldsProducer(featureFlagContext(tc.featureFlags, true), tc.expectedErr)
			require.Equal(t, tc.expectedFields, fields)
		})
	}
}

func featureFlagContext(flags map[featureflag.FeatureFlag]bool, alwaysLogFeatureFlags bool) context.Context {
	//nolint:forbidigo // This test tests feature flags. We want context to be in a clean state
	ctx := context.Background()
	for flag, value := range flags {
		ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, flag, value)
	}
	return featureflag.IncomingCtxWithFeatureFlag(ctx, featureflag.AlwaysLogFeatureFlags, alwaysLogFeatureFlags)
}
