package sentryhandler

import (
	"context"
	"fmt"
	"testing"
	"time"

	sentry "github.com/getsentry/sentry-go"
	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGenerateSentryEvent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		ctx           context.Context
		method        string
		duration      time.Duration
		err           error
		expectedEvent *sentry.Event
	}{
		{
			desc:     "internal error",
			method:   "/gitaly.SSHService/SSHUploadPack",
			duration: 500 * time.Millisecond,
			err:      fmt.Errorf("Internal"),
			expectedEvent: &sentry.Event{
				Message:     "Internal",
				Transaction: "SSHService::SSHUploadPack",
				Tags: map[string]string{
					"grpc.code":    "Unknown",
					"grpc.method":  "/gitaly.SSHService/SSHUploadPack",
					"grpc.time_ms": "500",
					"system":       "grpc",
				},
				Fingerprint: []string{
					"grpc", "SSHService::SSHUploadPack", "Unknown",
				},
				Exception: []sentry.Exception{
					{
						Type:  "*errors.errorString",
						Value: "Internal",
					},
				},
				Extra:    map[string]any{},
				Contexts: map[string]map[string]any{},
				Modules:  map[string]string{},
			},
		},
		{
			desc:     "GRPC error",
			method:   "/gitaly.RepoService/RepoExists",
			duration: 500 * time.Millisecond,
			err:      status.Errorf(codes.NotFound, "Something failed"),
			expectedEvent: &sentry.Event{
				Message:     "rpc error: code = NotFound desc = Something failed",
				Transaction: "RepoService::RepoExists",
				Tags: map[string]string{
					"grpc.code":    "NotFound",
					"grpc.method":  "/gitaly.RepoService/RepoExists",
					"grpc.time_ms": "500",
					"system":       "grpc",
				},
				Fingerprint: []string{
					"grpc", "RepoService::RepoExists", "NotFound",
				},
				Exception: []sentry.Exception{
					{
						Type:  "*status.Error",
						Value: "rpc error: code = NotFound desc = Something failed",
					},
				},
				Extra:    map[string]any{},
				Contexts: map[string]map[string]any{},
				Modules:  map[string]string{},
			},
		},
		{
			desc:          "successful call is ignored",
			method:        "/gitaly.RepoService/RepoExists",
			err:           nil,
			expectedEvent: nil,
		},
		{
			desc:          "TreeEntry with NotFound error is ignored",
			method:        "/gitaly.CommitService/TreeEntry",
			err:           status.Errorf(codes.NotFound, "Path not found"),
			expectedEvent: nil,
		},
		{
			desc:          "Canceled error is ignored",
			method:        "/gitaly.RepoService/RepoExists",
			err:           status.Errorf(codes.Canceled, "Something failed"),
			expectedEvent: nil,
		},
		{
			desc:          "DeadlineExceeded erorr is ignored",
			method:        "/gitaly.RepoService/RepoExists",
			err:           status.Errorf(codes.DeadlineExceeded, "Something failed"),
			expectedEvent: nil,
		},
		{
			desc:          "FailedPrecondition error is ignored",
			method:        "/gitaly.RepoService/RepoExists",
			err:           status.Errorf(codes.FailedPrecondition, "Something failed"),
			expectedEvent: nil,
		},
		{
			desc: "error that is not marked to be skipped is not ignored",
			ctx: func() context.Context {
				var result context.Context

				ctx := testhelper.Context(t)

				// this is the only way how we could populate context with `tags` assembler
				_, err := grpcmwtags.UnaryServerInterceptor()(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
					result = ctx
					return nil, nil
				})
				require.NoError(t, err)
				return result
			}(),
			duration: 500 * time.Millisecond,
			method:   "/gitaly.RepoService/RepoExists",
			err:      status.Errorf(codes.NotFound, "Something failed"),
			expectedEvent: &sentry.Event{
				Message:     "rpc error: code = NotFound desc = Something failed",
				Transaction: "RepoService::RepoExists",
				Tags: map[string]string{
					"grpc.code":    "NotFound",
					"grpc.method":  "/gitaly.RepoService/RepoExists",
					"grpc.time_ms": "500",
					"system":       "grpc",
				},
				Fingerprint: []string{
					"grpc", "RepoService::RepoExists", "NotFound",
				},
				Exception: []sentry.Exception{
					{
						Type:  "*status.Error",
						Value: "rpc error: code = NotFound desc = Something failed",
					},
				},
				Extra:    map[string]any{},
				Contexts: map[string]map[string]any{},
				Modules:  map[string]string{},
			},
		},
		{
			desc: "error that is marked to be skipped is ignored",
			ctx: func() context.Context {
				var result context.Context

				ctx := testhelper.Context(t)

				// this is the only way how we could populate context with `tags` assembler
				_, err := grpcmwtags.UnaryServerInterceptor()(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
					result = ctx
					return nil, nil
				})
				require.NoError(t, err)
				MarkToSkip(result)
				return result
			}(),
			method:        "/gitaly.RepoService/RepoExists",
			err:           status.Errorf(codes.NotFound, "Something failed"),
			expectedEvent: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = testhelper.Context(t)
			}

			event := generateSentryEvent(ctx, tc.method, tc.duration, tc.err)
			require.Equal(t, tc.expectedEvent, event)
		})
	}
}
