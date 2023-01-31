package statushandler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnary(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	timeoutCtx, timeout := context.WithTimeout(ctx, 0) //nolint:forbidigo
	timeout()

	for desc, tc := range map[string]struct {
		ctx         context.Context
		err         error
		expectedErr error
	}{
		"context cancelled": {
			ctx: cancelledCtx,
		},
		"context timeout": {
			ctx: timeoutCtx,
		},
		"context cancelled with an error returned": {
			ctx:         cancelledCtx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.Canceled, assert.AnError.Error()),
		},
		"context timed out with an error returned": {
			ctx:         timeoutCtx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.DeadlineExceeded, assert.AnError.Error()),
		},
		"bare error": {
			ctx:         ctx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.Internal, assert.AnError.Error()),
		},
		"wrapped error": {
			ctx:         ctx,
			err:         structerr.NewInvalidArgument("%w", assert.AnError),
			expectedErr: status.Error(codes.InvalidArgument, assert.AnError.Error()),
		},
		"formatted wrapped error": {
			ctx:         ctx,
			err:         fmt.Errorf("cause: %w", structerr.NewInvalidArgument("%w", assert.AnError)),
			expectedErr: status.Error(codes.InvalidArgument, "cause: "+assert.AnError.Error()),
		},
		"cancelled error": {
			ctx:         ctx,
			err:         context.Canceled,
			expectedErr: status.Error(codes.Internal, context.Canceled.Error()),
		},
		"timeout error": {
			ctx:         ctx,
			err:         context.DeadlineExceeded,
			expectedErr: status.Error(codes.Internal, context.DeadlineExceeded.Error()),
		},
		"no errors": {
			ctx:         ctx,
			expectedErr: status.New(codes.OK, "").Err(),
		},
	} {
		t.Run(desc, func(t *testing.T) {
			_, err := Unary(tc.ctx, nil, nil, func(context.Context, interface{}) (interface{}, error) {
				return nil, tc.err
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestStream(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	timedoutCtx, timeout := context.WithTimeout(ctx, 0) //nolint:forbidigo
	timeout()

	for desc, tc := range map[string]struct {
		ctx         context.Context
		err         error
		expectedErr error
	}{
		"context cancelled": {
			ctx: cancelledCtx,
		},
		"context timed out": {
			ctx: timedoutCtx,
		},
		"context cancelled with an error returned": {
			ctx:         cancelledCtx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.Canceled, assert.AnError.Error()),
		},
		"context timed out with an error returned": {
			ctx:         timedoutCtx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.DeadlineExceeded, assert.AnError.Error()),
		},
		"bare error": {
			ctx:         ctx,
			err:         assert.AnError,
			expectedErr: status.Error(codes.Internal, assert.AnError.Error()),
		},
		"wrapped error": {
			ctx:         ctx,
			err:         structerr.NewInvalidArgument("%w", assert.AnError),
			expectedErr: status.Error(codes.InvalidArgument, assert.AnError.Error()),
		},
		"formatted wrapped error": {
			ctx:         ctx,
			err:         fmt.Errorf("cause: %w", structerr.NewInvalidArgument("%w", assert.AnError)),
			expectedErr: status.Error(codes.InvalidArgument, "cause: "+assert.AnError.Error()),
		},
		"cancelled error": {
			ctx:         ctx,
			err:         context.Canceled,
			expectedErr: status.Error(codes.Internal, context.Canceled.Error()),
		},
		"timeout error": {
			ctx:         ctx,
			err:         context.DeadlineExceeded,
			expectedErr: status.Error(codes.Internal, context.DeadlineExceeded.Error()),
		},
		"no errors": {
			ctx:         ctx,
			expectedErr: status.New(codes.OK, "").Err(),
		},
	} {
		t.Run(desc, func(t *testing.T) {
			err := Stream(nil, serverStream{ctx: tc.ctx}, nil, func(srv interface{}, stream grpc.ServerStream) error {
				return tc.err
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

type serverStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss serverStream) Context() context.Context {
	return ss.ctx
}
