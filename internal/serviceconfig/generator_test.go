package serviceconfig

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGenerate(t *testing.T) {
	t.Parallel()

	commonRetryPolicy := &gitalypb.MethodConfig_RetryPolicy{
		MaxAttempts:          3,
		InitialBackoff:       &durationpb.Duration{Nanos: 100000000},
		MaxBackoff:           &durationpb.Duration{Seconds: 1},
		BackoffMultiplier:    2,
		RetryableStatusCodes: []string{"UNAVAILABLE", "DEADLINE_EXCEEDED"},
	}

	for _, tc := range []struct {
		name                  string
		autoRetry             bool
		expectedServiceConfig *gitalypb.ServiceConfig
	}{
		{
			name:      "without autoRetry option",
			autoRetry: false,
			expectedServiceConfig: &gitalypb.ServiceConfig{
				LoadBalancingConfig: []*gitalypb.LoadBalancingConfig{{
					Policy: &gitalypb.LoadBalancingConfig_RoundRobin{},
				}},
			},
		},
		{
			name:      "with autoRetry option",
			autoRetry: true,
			expectedServiceConfig: &gitalypb.ServiceConfig{
				LoadBalancingConfig: []*gitalypb.LoadBalancingConfig{{
					Policy: &gitalypb.LoadBalancingConfig_RoundRobin{},
				}},
				MethodConfig: []*gitalypb.MethodConfig{
					{
						Name: []*gitalypb.MethodConfig_Name{
							{
								Service: "gitaly.SSHService",
								Method:  "SSHUploadArchive",
							},
							{
								Service: "gitaly.SSHService",
								Method:  "SSHUploadPack",
							},
							{
								Service: "gitaly.SSHService",
								Method:  "SSHUploadPackWithSidechannel",
							},
						},
						RetryPolicy: commonRetryPolicy,
					},
					{
						Name: []*gitalypb.MethodConfig_Name{
							{
								Service: "gitaly.SmartHTTPService",
								Method:  "InfoRefsReceivePack",
							},
							{
								Service: "gitaly.SmartHTTPService",
								Method:  "InfoRefsUploadPack",
							},
							{
								Service: "gitaly.SmartHTTPService",
								Method:  "PostUploadPack",
							},
							{
								Service: "gitaly.SmartHTTPService",
								Method:  "PostUploadPackWithSidechannel",
							},
						},
						RetryPolicy: commonRetryPolicy,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := protoregistry.NewFromPaths(
				"smarthttp.proto",
				"ssh.proto",
			)
			require.NoError(t, err)

			config := Generate(registry, tc.autoRetry)
			require.Equal(t, tc.expectedServiceConfig, config)
		})
	}
}

func TestGenerate_withAllProtos(t *testing.T) {
	t.Parallel()

	registry := protoregistry.GitalyProtoPreregistered
	config := Generate(registry, true)
	for _, methodConfig := range config.MethodConfig {
		for _, methodName := range methodConfig.GetName() {
			fullName := fmt.Sprintf("/%s/%s", methodName.GetService(), methodName.GetMethod())

			mth, err := registry.LookupMethod(fullName)
			require.NoError(t, err)
			require.Equal(t, mth.Operation, protoregistry.OpAccessor)
		}
	}
}
