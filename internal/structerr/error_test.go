package structerr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// unusedErrorCode is any error code that we don't have any constructors for yet. This is used
// to verify that we correctly wrap errors that already have a different gRPC error code than the
// one under test.
const unusedErrorCode = codes.OutOfRange

func TestNew(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		constructor  func(format string, a ...any) Error
		expectedCode codes.Code
	}{
		{
			desc:         "New",
			constructor:  New,
			expectedCode: codes.Internal,
		},
		{
			desc:         "NewAborted",
			constructor:  NewAborted,
			expectedCode: codes.Aborted,
		},
		{
			desc:         "NewAlreadyExist",
			constructor:  NewAlreadyExists,
			expectedCode: codes.AlreadyExists,
		},
		{
			desc:         "NewCanceled",
			constructor:  NewCanceled,
			expectedCode: codes.Canceled,
		},
		{
			desc:         "NewDataLoss",
			constructor:  NewDataLoss,
			expectedCode: codes.DataLoss,
		},
		{
			desc:         "NewDeadlineExceeded",
			constructor:  NewDeadlineExceeded,
			expectedCode: codes.DeadlineExceeded,
		},
		{
			desc:         "NewFailedPrecondition",
			constructor:  NewFailedPrecondition,
			expectedCode: codes.FailedPrecondition,
		},
		{
			desc:         "NewInternal",
			constructor:  NewInternal,
			expectedCode: codes.Internal,
		},
		{
			desc:         "NewInvalidArgument",
			constructor:  NewInvalidArgument,
			expectedCode: codes.InvalidArgument,
		},
		{
			desc:         "NewNotFound",
			constructor:  NewNotFound,
			expectedCode: codes.NotFound,
		},
		{
			desc:         "NewPermissionDenied",
			constructor:  NewPermissionDenied,
			expectedCode: codes.PermissionDenied,
		},
		{
			desc:         "NewResourceExhausted",
			constructor:  NewResourceExhausted,
			expectedCode: codes.ResourceExhausted,
		},
		{
			desc:         "NewUnavailable",
			constructor:  NewUnavailable,
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "NewUnauthenticated",
			constructor:  NewUnauthenticated,
			expectedCode: codes.Unauthenticated,
		},
		{
			desc:         "NewUnimplemented",
			constructor:  NewUnimplemented,
			expectedCode: codes.Unimplemented,
		},
		{
			desc:         "NewUnknown",
			constructor:  NewUnknown,
			expectedCode: codes.Unknown,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NotEqual(t, tc.expectedCode, unusedErrorCode)

			t.Run("without wrapping", func(t *testing.T) {
				err := tc.constructor("top-level: %v", errors.New("nested"))
				require.EqualError(t, err, "top-level: nested")
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("wrapping normal error", func(t *testing.T) {
				err := tc.constructor("top-level: %w", errors.New("nested"))
				require.EqualError(t, err, "top-level: nested")
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("wrapping structerr with %v", func(t *testing.T) {
				err := tc.constructor("top-level: %v", newError(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")
				// We should be reporting the error code of the newly created error.
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("wrapping structerr with %w", func(t *testing.T) {
				err := tc.constructor("top-level: %w", newError(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")
				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: nested"), s)
			})

			t.Run("wrapping status.Error", func(t *testing.T) {
				err := tc.constructor("top-level: %w", status.Error(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")
				// We should be reporting the error code of the wrapped gRPC status.
				require.Equal(t, unusedErrorCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: nested"), s)
			})

			t.Run("mixed normal and structerr chain", func(t *testing.T) {
				err := tc.constructor("first: %w", fmt.Errorf("second: %w", newError(unusedErrorCode, "third")))
				require.EqualError(t, err, "first: second: third")
				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "first: second: third"), s)
			})
		})
	}
}
