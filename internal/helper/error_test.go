package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/grpc_testing"
	"google.golang.org/protobuf/types/known/anypb"
)

// unusedErrorCode is any error code that we don't have any wrapping functions for yet. This is used
// to verify that we correctly wrap errors that already have a different gRPC error code than the
// one under test.
const unusedErrorCode = codes.OutOfRange

func TestErrorf(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		errorf       func(format string, a ...interface{}) error
		expectedCode codes.Code
	}{
		{
			desc:         "Internalf",
			errorf:       ErrInternalf,
			expectedCode: codes.Internal,
		},
		{
			desc:         "InvalidArgumentf",
			errorf:       ErrInvalidArgumentf,
			expectedCode: codes.InvalidArgument,
		},
		{
			desc:         "FailedPreconditionf",
			errorf:       ErrFailedPreconditionf,
			expectedCode: codes.FailedPrecondition,
		},
		{
			desc:         "NotFoundf",
			errorf:       ErrNotFoundf,
			expectedCode: codes.NotFound,
		},
		{
			desc:         "ErrUnauthenticatedf",
			errorf:       ErrUnauthenticatedf,
			expectedCode: codes.Unauthenticated,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("with non-gRPC error", func(t *testing.T) {
				err := tc.errorf("top-level: %w", errors.New("nested"))
				require.EqualError(t, err, "top-level: nested")
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("with status.Errorf error and %v", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %v", status.Errorf(unusedErrorCode, "deeply: %s", "nested"))
				require.EqualError(t, err, "top-level: deeply: nested")

				// The error code of the nested error should be discarded.
				require.Equal(t, tc.expectedCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: deeply: nested"), s)
			})

			t.Run("with status.Errorf error and %w", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %w", status.Errorf(unusedErrorCode, "deeply: %s", "nested"))
				require.EqualError(t, err, "top-level: deeply: nested")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: deeply: nested"), s)
			})

			t.Run("with status.Error error and %v", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %v", status.Error(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")

				// The error code of the nested error should be discarded.
				require.Equal(t, tc.expectedCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("with status.Error error and %w", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %w", status.Error(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: nested"), s)
			})

			t.Run("multi-nesting gRPC errors", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("first: %w",
					ErrInternalf("second: %w",
						status.Error(unusedErrorCode, "third"),
					),
				)
				require.EqualError(t, err, "first: second: third")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "first: second: third"), s)
			})

			t.Run("multi-nesting with standard error wrapping", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("first: %w",
					fmt.Errorf("second: %w",
						formatError(unusedErrorCode, "third"),
					),
				)
				require.EqualError(t, err, "first: second: third")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "first: second: third"), s)
			})

			t.Run("wrapping formatted gRPC error with details", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				marshaledDetail, err := anypb.New(&grpc_testing.Payload{
					Body: []byte("contents"),
				})
				require.NoError(t, err)

				proto := status.New(unusedErrorCode, "details").Proto()
				proto.Details = []*anypb.Any{marshaledDetail}
				errWithDetails := status.ErrorProto(proto)

				err = tc.errorf("top: %w", fmt.Errorf("detailed: %w", errWithDetails))
				// We can't do anything about the "rpc error:" part in the middle as
				// this is put there by `fmt.Errorf()` already.
				require.EqualError(t, err, "top: detailed: rpc error: code = OutOfRange desc = details")
				// We should be reporting the error code of the wrapped gRPC status.
				require.Equal(t, unusedErrorCode, status.Code(err))

				expectedErr, marshallingErr := status.New(unusedErrorCode, "top: detailed: rpc error: code = OutOfRange desc = details").WithDetails(&grpc_testing.Payload{
					Body: []byte("contents"),
				})
				require.NoError(t, marshallingErr)

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, expectedErr, s)
			})
		})
	}
}

func TestGrpcCode(t *testing.T) {
	t.Parallel()
	for desc, tc := range map[string]struct {
		in  error
		exp codes.Code
	}{
		"unwrapped status": {
			in:  status.Error(codes.NotFound, ""),
			exp: codes.NotFound,
		},
		"wrapped status": {
			in:  fmt.Errorf("context: %w", status.Error(codes.NotFound, "")),
			exp: codes.NotFound,
		},
		"unwrapped status created by helpers": {
			in:  ErrNotFoundf(""),
			exp: codes.NotFound,
		},
		"wrapped status created by helpers": {
			in:  fmt.Errorf("context: %w", ErrNotFoundf("")),
			exp: codes.NotFound,
		},
		"double wrapped status created by helpers": {
			in:  fmt.Errorf("outer: %w", fmt.Errorf("context: %w", ErrNotFoundf(""))),
			exp: codes.NotFound,
		},
		"double helper wrapped status": {
			in:  ErrFailedPreconditionf("outer: %w", fmt.Errorf("context: %w", ErrNotFoundf(""))),
			exp: codes.NotFound,
		},
		"nil input": {
			in:  nil,
			exp: codes.OK,
		},
		"no code defined": {
			in:  assert.AnError,
			exp: codes.Unknown,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			assert.Equal(t, tc.exp, GrpcCode(tc.in))
		})
	}
}
