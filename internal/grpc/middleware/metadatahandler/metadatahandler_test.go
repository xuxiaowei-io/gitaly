package metadatahandler

import (
	"context"
	"testing"
	"time"

	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

const (
	correlationID = "CORRELATION_ID"
	clientName    = "CLIENT_NAME"
)

func TestAddMetadataTags(t *testing.T) {
	t.Parallel()

	baseContext := testhelper.Context(t)

	for _, tc := range []struct {
		desc             string
		metadata         metadata.MD
		deadline         bool
		expectedMetatags metadataTags
	}{
		{
			desc:     "empty metadata",
			metadata: metadata.Pairs(),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "context containing metadata",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     "testsite",
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "context containing metadata and a deadline",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     "testsite",
				authVersion:  unknownValue,
				deadlineType: unknownValue,
			},
		},
		{
			desc:     "context containing metadata and a deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "regular",
			},
		},
		{
			desc:     "a context without deadline but with deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "with a context containing metadata",
			metadata: metadata.Pairs("deadline_type", "regular", "client_name", "rails"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   "rails",
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "regular",
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := metadata.NewIncomingContext(baseContext, tc.metadata)
			if tc.deadline {
				var cancel func()
				//nolint:forbidigo // We explicitly need to test whether deadlines
				// propagate as expected.
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(50*time.Millisecond))
				defer cancel()
			}

			require.Equal(t, tc.expectedMetatags, addMetadataTags(ctx, "unary"))
		})
	}
}

func TestGRPCTags(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx = metadata.NewIncomingContext(
		correlation.ContextWithCorrelation(
			correlation.ContextWithClientName(
				ctx,
				clientName,
			),
			correlationID,
		),
		metadata.Pairs(),
	)

	interceptor := grpcmwtags.UnaryServerInterceptor()

	_, err := interceptor(ctx, nil, nil, func(ctx context.Context, _ interface{}) (interface{}, error) {
		metaTags := addMetadataTags(ctx, "unary")

		require.Equal(t, metadataTags{
			clientName:   clientName,
			callSite:     "unknown",
			authVersion:  "unknown",
			deadlineType: "none",
		}, metaTags)

		require.Equal(t, map[string]interface{}{
			"correlation_id": correlationID,
			ClientNameKey:    clientName,
			DeadlineTypeKey:  "none",
			MethodTypeKey:    "unary",
		}, grpcmwtags.Extract(ctx).Values())

		return nil, nil
	})
	require.NoError(t, err)
}

func TestExtractServiceAndMethodName(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc            string
		fullMethodName  string
		expectedService string
		expectedMethod  string
	}{
		{
			desc:            "blank",
			fullMethodName:  "",
			expectedService: unknownValue,
			expectedMethod:  unknownValue,
		},
		{
			desc:            "normal",
			fullMethodName:  "/gitaly.OperationService/method",
			expectedService: "gitaly.OperationService",
			expectedMethod:  "method",
		},
		{
			desc:            "malformed",
			fullMethodName:  "//method",
			expectedService: "",
			expectedMethod:  "method",
		},
		{
			desc:            "malformed",
			fullMethodName:  "/gitaly.OperationService/",
			expectedService: "gitaly.OperationService",
			expectedMethod:  "",
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			service, method := extractServiceAndMethodName(tc.fullMethodName)
			require.Equal(t, tc.expectedService, service)
			require.Equal(t, tc.expectedMethod, method)
		})
	}
}
