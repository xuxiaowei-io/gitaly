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
		fullMethod       string
		metadata         metadata.MD
		deadline         bool
		expectedMetatags metadataTags
	}{
		{
			desc:     "empty metadata",
			metadata: metadata.Pairs(),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:     "context containing metadata",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        "testsite",
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:     "context containing metadata and a deadline",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        "testsite",
				authVersion:     unknownValue,
				deadlineType:    unknownValue,
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:     "context containing metadata and a deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "regular",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:     "a context without deadline but with deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:     "with a context containing metadata",
			metadata: metadata.Pairs("deadline_type", "regular", "client_name", "rails"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:      "rails",
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "regular",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:       "with unknown method",
			fullMethod: "/gitaly.RepositoryService/UnknownMethod",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: unknownValue,
				methodScope:     unknownValue,
			},
		},
		{
			desc:       "with repository-scoped accessor",
			fullMethod: "/gitaly.RepositoryService/ObjectFormat",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: "accessor",
				methodScope:     "repository",
			},
		},
		{
			desc:       "with repository-scoped mutator",
			fullMethod: "/gitaly.RepositoryService/CreateRepository",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: "mutator",
				methodScope:     "repository",
			},
		},
		{
			desc:       "with repository-scoped maintenance",
			fullMethod: "/gitaly.RepositoryService/OptimizeRepository",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: "maintenance",
				methodScope:     "repository",
			},
		},
		{
			desc:       "with repository-scoped maintenance",
			fullMethod: "/gitaly.RepositoryService/OptimizeRepository",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: "maintenance",
				methodScope:     "repository",
			},
		},
		{
			desc:       "with storage-scoped accessor",
			fullMethod: "/gitaly.RemoteService/FindRemoteRepository",
			metadata:   metadata.Pairs(),
			deadline:   false,
			expectedMetatags: metadataTags{
				clientName:      unknownValue,
				callSite:        unknownValue,
				authVersion:     unknownValue,
				deadlineType:    "none",
				methodOperation: "accessor",
				methodScope:     "storage",
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

			require.Equal(t, tc.expectedMetatags, addMetadataTags(ctx, tc.fullMethod, "unary"))
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
		metaTags := addMetadataTags(ctx, "/gitaly.RepositoryService/OptimizeRepository", "unary")

		require.Equal(t, metadataTags{
			clientName:      clientName,
			callSite:        "unknown",
			authVersion:     "unknown",
			deadlineType:    "none",
			methodOperation: "maintenance",
			methodScope:     "repository",
		}, metaTags)

		require.Equal(t, map[string]interface{}{
			"correlation_id":   correlationID,
			ClientNameKey:      clientName,
			DeadlineTypeKey:    "none",
			MethodTypeKey:      "unary",
			MethodOperationKey: "maintenance",
			MethodScopeKey:     "repository",
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
