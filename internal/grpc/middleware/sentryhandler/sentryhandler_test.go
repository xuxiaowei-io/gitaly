package sentryhandler

import (
	"context"
	"fmt"
	"testing"
	"time"

	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGenerateSentryEvent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		ctx         context.Context
		method      string
		duration    time.Duration
		wantNil     bool
		err         error
		wantCode    codes.Code
		wantMessage string
		wantCulprit string
	}{
		{
			desc:        "internal error",
			method:      "/gitaly.SSHService/SSHUploadPack",
			duration:    500 * time.Millisecond,
			err:         fmt.Errorf("Internal"),
			wantCode:    codes.Unknown,
			wantMessage: "Internal",
			wantCulprit: "SSHService::SSHUploadPack",
		},
		{
			desc:        "GRPC error",
			method:      "/gitaly.RepoService/RepoExists",
			duration:    500 * time.Millisecond,
			err:         status.Errorf(codes.NotFound, "Something failed"),
			wantCode:    codes.NotFound,
			wantMessage: "rpc error: code = NotFound desc = Something failed",
			wantCulprit: "RepoService::RepoExists",
		},
		{
			desc:     "GRPC error",
			method:   "/gitaly.CommitService/TreeEntry",
			duration: 500 * time.Millisecond,
			err:      status.Errorf(codes.NotFound, "Path not found"),
			wantNil:  true,
		},
		{
			desc:     "nil",
			method:   "/gitaly.RepoService/RepoExists",
			duration: 500 * time.Millisecond,
			err:      nil,
			wantNil:  true,
		},
		{
			desc:     "Canceled",
			method:   "/gitaly.RepoService/RepoExists",
			duration: 500 * time.Millisecond,
			err:      status.Errorf(codes.Canceled, "Something failed"),
			wantNil:  true,
		},
		{
			desc:     "DeadlineExceeded",
			method:   "/gitaly.RepoService/RepoExists",
			duration: 500 * time.Millisecond,
			err:      status.Errorf(codes.DeadlineExceeded, "Something failed"),
			wantNil:  true,
		},
		{
			desc:     "FailedPrecondition",
			method:   "/gitaly.RepoService/RepoExists",
			duration: 500 * time.Millisecond,
			err:      status.Errorf(codes.FailedPrecondition, "Something failed"),
			wantNil:  true,
		},
		{
			desc: "marked to skip",
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
			wantNil: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = testhelper.Context(t)
			}

			event := generateSentryEvent(ctx, tc.method, tc.duration, tc.err)

			if tc.wantNil {
				assert.Nil(t, event)
				return
			}

			require.NotNil(t, event)
			assert.Equal(t, tc.wantCulprit, event.Transaction)
			assert.Equal(t, tc.wantMessage, event.Message)
			assert.Equal(t, event.Tags["system"], "grpc")
			assert.NotEmpty(t, event.Tags["grpc.time_ms"])
			assert.Equal(t, tc.method, event.Tags["grpc.method"])
			assert.Equal(t, tc.wantCode.String(), event.Tags["grpc.code"])
			assert.Equal(t, []string{"grpc", tc.wantCulprit, tc.wantCode.String()}, event.Fingerprint)
		})
	}
}
