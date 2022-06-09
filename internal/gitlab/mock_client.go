package gitlab

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
)

var (
	// MockAllowed is a callback for the MockClient's `Allowed()` function which always allows a
	// change.
	MockAllowed = func(context.Context, AllowedParams) (bool, string, error) {
		return true, "", nil
	}
	// MockPreReceive is a callback for the MockClient's `PreReceive()` function which always
	// allows a change.
	MockPreReceive = func(context.Context, string) (bool, error) {
		return true, nil
	}
	// MockPostReceive is a callback for the MockCLient's `PostReceive()` function which always
	// allows a change.
	MockPostReceive = func(context.Context, string, string, string, ...string) (bool, []PostReceiveMessage, error) {
		return true, nil, nil
	}

	// MockFeatures is a callback for the MockClient's `Features()` function
	// which always returns nothing.
	MockFeatures = func(ctx context.Context) (map[featureflag.FeatureFlag]bool, error) {
		return nil, nil
	}
)

// MockClient is a mock client of the internal GitLab API.
type MockClient struct {
	tb          testing.TB
	allowed     func(context.Context, AllowedParams) (bool, string, error)
	preReceive  func(context.Context, string) (bool, error)
	postReceive func(context.Context, string, string, string, ...string) (bool, []PostReceiveMessage, error)
	features    func(context.Context) (map[featureflag.FeatureFlag]bool, error)
}

// NewMockClient returns a new mock client for the internal GitLab API.
func NewMockClient(
	tb testing.TB,
	allowed func(context.Context, AllowedParams) (bool, string, error),
	preReceive func(context.Context, string) (bool, error),
	postReceive func(context.Context, string, string, string, ...string) (bool, []PostReceiveMessage, error),
	features func(context.Context) (map[featureflag.FeatureFlag]bool, error),
) Client {
	return &MockClient{
		tb:          tb,
		allowed:     allowed,
		preReceive:  preReceive,
		postReceive: postReceive,
		features:    features,
	}
}

// Allowed does nothing and always returns true.
func (m *MockClient) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	require.NotNil(m.tb, m.allowed, "allowed called but not set")
	return m.allowed(ctx, params)
}

// Check does nothing and always returns a CheckInfo prepopulated with static data.
func (m *MockClient) Check(ctx context.Context) (*CheckInfo, error) {
	return &CheckInfo{
		Version:        "v13.5.0",
		Revision:       "deadbeef",
		APIVersion:     "v4",
		RedisReachable: true,
	}, nil
}

// PreReceive does nothing and always return true.
func (m *MockClient) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	require.NotNil(m.tb, m.preReceive, "preReceive called but not set")
	return m.preReceive(ctx, glRepository)
}

// PostReceive does nothing and always returns true.
func (m *MockClient) PostReceive(ctx context.Context, glRepository, glID, changes string, gitPushOptions ...string) (bool, []PostReceiveMessage, error) {
	require.NotNil(m.tb, m.postReceive, "postReceive called but not set")
	return m.postReceive(ctx, glRepository, glID, changes, gitPushOptions...)
}

// Features does nothing and always return an empty slice of Features.
func (m *MockClient) Features(ctx context.Context) (map[featureflag.FeatureFlag]bool, error) {
	require.NotNil(m.tb, m.features, "features called but not set")
	return m.features(ctx)
}
