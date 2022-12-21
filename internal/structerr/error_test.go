package structerr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/grpc_testing"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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

			t.Run("wrapping formatted gRPC error", func(t *testing.T) {
				err := tc.constructor("top: %w", fmt.Errorf("middle: %w", status.Error(unusedErrorCode, "bottom")))
				// We can't do anything about the "rpc error:" part in the middle as
				// this is put there by `fmt.Errorf()` already.
				require.EqualError(t, err, "top: middle: rpc error: code = OutOfRange desc = bottom")
				// We should be reporting the error code of the wrapped gRPC status.
				require.Equal(t, unusedErrorCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top: middle: rpc error: code = OutOfRange desc = bottom"), s)
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

			t.Run("wrapping formatted gRPC error with details", func(t *testing.T) {
				marshaledDetail, err := anypb.New(&grpc_testing.Payload{
					Body: []byte("contents"),
				})
				require.NoError(t, err)

				proto := status.New(unusedErrorCode, "details").Proto()
				proto.Details = []*anypb.Any{marshaledDetail}
				errWithDetails := status.ErrorProto(proto)

				err = tc.constructor("top: %w", fmt.Errorf("detailed: %w", errWithDetails))
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

			t.Run("overriding gRPC code", func(t *testing.T) {
				err := tc.constructor("message").WithGRPCCode(unusedErrorCode)
				require.EqualError(t, err, "message")
				require.Equal(t, unusedErrorCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "message"), s)
			})
		})
	}
}

func TestError_Is(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc                string
		err                 Error
		targetErr           error
		expectedIs          bool
		expectedErrIsTarget bool
		expectedTargetIsErr bool
	}{
		{
			desc:                "same error",
			err:                 New("same"),
			targetErr:           New("same"),
			expectedIs:          true,
			expectedErrIsTarget: true,
			expectedTargetIsErr: true,
		},
		{
			desc:                "different error types",
			err:                 New("same"),
			targetErr:           errors.New("same"),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: false,
		},
		{
			desc:                "different error codes",
			err:                 NewInternal("same"),
			targetErr:           NewAlreadyExists("same"),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: false,
		},
		{
			desc:                "different error message",
			err:                 New("a"),
			targetErr:           New("b"),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: false,
		},
		{
			desc: "different error details",
			err: New("same").WithDetail(&grpc_testing.Payload{
				Body: []byte("a"),
			}),
			targetErr: New("same").WithDetail(&grpc_testing.Payload{
				Body: []byte("b"),
			}),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: false,
		},
		{
			desc:                "wrapped error",
			err:                 New("same"),
			targetErr:           fmt.Errorf("toplevel: %w", New("same")),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: true,
		},
		{
			desc:                "wrapped structerr",
			err:                 New("same"),
			targetErr:           New("toplevel: %w", New("same")),
			expectedIs:          false,
			expectedErrIsTarget: false,
			expectedTargetIsErr: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("direct match", func(t *testing.T) {
				require.Equal(t, tc.expectedIs, tc.err.Is(tc.targetErr))
			})

			t.Run("err is target", func(t *testing.T) {
				require.Equal(t, tc.expectedErrIsTarget, errors.Is(tc.err, tc.targetErr))
			})

			t.Run("target is err", func(t *testing.T) {
				require.Equal(t, tc.expectedTargetIsErr, errors.Is(tc.targetErr, tc.err))
			})
		})
	}
}

func TestError_Metadata(t *testing.T) {
	t.Parallel()

	t.Run("without metadata", func(t *testing.T) {
		err := New("message")
		require.Equal(t, Error{
			err:  errors.New("message"),
			code: codes.Internal,
		}, err)
		require.Equal(t, map[string]any{}, err.Metadata())
	})

	t.Run("single metadata key", func(t *testing.T) {
		err := New("message").WithMetadata("key", "value")
		require.Equal(t, Error{
			err:  errors.New("message"),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "key", value: "value"},
			},
		}, err)
		require.Equal(t, map[string]any{
			"key": "value",
		}, err.Metadata())
	})

	t.Run("multiple metadata keys", func(t *testing.T) {
		err := New("message").WithMetadata("first", 1).WithMetadata("second", 2)
		require.Equal(t, Error{
			err:  errors.New("message"),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "first", value: 1},
				{key: "second", value: 2},
			},
		}, err)
		require.Equal(t, map[string]any{
			"first":  1,
			"second": 2,
		}, err.Metadata())
	})

	t.Run("overriding metadata keys", func(t *testing.T) {
		err := New("message").WithMetadata("first", "initial").WithMetadata("first", "overridden")
		require.Equal(t, Error{
			err:  errors.New("message"),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "first", value: "overridden"},
			},
		}, err)
		require.Equal(t, map[string]any{
			"first": "overridden",
		}, err.Metadata())
	})

	t.Run("chained metadata", func(t *testing.T) {
		nestedErr := New("nested").WithMetadata("nested", "value")
		toplevelErr := New("top-level: %w", nestedErr).WithMetadata("toplevel", "value")
		require.Equal(t, Error{
			err:  fmt.Errorf("top-level: %w", nestedErr),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "toplevel", value: "value"},
			},
		}, toplevelErr)
		require.Equal(t, map[string]any{
			"nested":   "value",
			"toplevel": "value",
		}, toplevelErr.Metadata())
	})

	t.Run("chained metadata overriding each other", func(t *testing.T) {
		nestedErr := New("nested").WithMetadata("key", "nested")
		toplevelErr := New("top-level: %w", nestedErr).WithMetadata("key", "top-level")
		require.Equal(t, Error{
			err:  fmt.Errorf("top-level: %w", nestedErr),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "key", value: "top-level"},
			},
		}, toplevelErr)
		require.Equal(t, map[string]any{
			"key": "top-level",
		}, toplevelErr.Metadata())
	})

	t.Run("chained metadata with internal overrides", func(t *testing.T) {
		nestedErr := New("nested").WithMetadata("nested", "initial").WithMetadata("nested", "overridden")
		toplevelErr := New("top-level: %w", nestedErr).WithMetadata("toplevel", "initial").WithMetadata("toplevel", "overridden")
		require.Equal(t, Error{
			err:  fmt.Errorf("top-level: %w", nestedErr),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "toplevel", value: "overridden"},
			},
		}, toplevelErr)
		require.Equal(t, map[string]any{
			"toplevel": "overridden",
			"nested":   "overridden",
		}, toplevelErr.Metadata())
	})

	t.Run("chained metadata with mixed error types", func(t *testing.T) {
		bottomErr := New("bottom").WithMetadata("bottom", "value")
		midlevelErr := fmt.Errorf("mid: %w", bottomErr)
		toplevelErr := New("top: %w", midlevelErr).WithMetadata("toplevel", "value")

		require.Equal(t, Error{
			err:  fmt.Errorf("top: %w", midlevelErr),
			code: codes.Internal,
			metadata: []metadataItem{
				{key: "toplevel", value: "value"},
			},
		}, toplevelErr)
		require.Equal(t, map[string]any{
			"bottom":   "value",
			"toplevel": "value",
		}, toplevelErr.Metadata())
	})
}

func TestError_Details(t *testing.T) {
	t.Parallel()

	initialPayload := &grpc_testing.Payload{
		Body: []byte("bottom"),
	}
	overridingPayload := &grpc_testing.Payload{
		Body: []byte("top"),
	}

	for _, tc := range []struct {
		desc            string
		createError     func() Error
		expectedErr     error
		expectedDetails []proto.Message
		expectedMessage string
	}{
		{
			desc: "without details",
			createError: func() Error {
				return New("message")
			},
			expectedErr: Error{
				err:  errors.New("message"),
				code: codes.Internal,
			},
			expectedMessage: "message",
		},
		{
			desc: "single detail",
			createError: func() Error {
				return New("message").WithDetail(initialPayload)
			},
			expectedErr: Error{
				err:  errors.New("message"),
				code: codes.Internal,
				details: []proto.Message{
					initialPayload,
				},
			},
			expectedDetails: []proto.Message{
				initialPayload,
			},
			expectedMessage: "message",
		},
		{
			desc: "overridden detail",
			createError: func() Error {
				return New("message").WithDetail(initialPayload).WithDetail(overridingPayload)
			},
			expectedErr: Error{
				err:  errors.New("message"),
				code: codes.Internal,
				details: []proto.Message{
					initialPayload,
					overridingPayload,
				},
			},
			expectedDetails: []proto.Message{
				initialPayload,
				overridingPayload,
			},
			expectedMessage: "message",
		},
		{
			desc: "chained details",
			createError: func() Error {
				nestedErr := New("nested").WithDetail(initialPayload)
				return New("top-level: %w", nestedErr).WithDetail(overridingPayload)
			},
			expectedErr: Error{
				err:  fmt.Errorf("top-level: %w", New("nested").WithDetail(initialPayload)),
				code: codes.Internal,
				details: []proto.Message{
					overridingPayload,
				},
			},
			expectedDetails: []proto.Message{
				overridingPayload,
				initialPayload,
			},
			expectedMessage: "top-level: nested",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.createError()
			require.Equal(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedDetails, err.Details())

			// `proto.Details()` returns an `[]any` slice, so we need to convert here or
			// otherwise the comparison would fail.
			anyDetails := make([]any, 0, len(tc.expectedDetails))
			for _, detail := range tc.expectedDetails {
				anyDetails = append(anyDetails, detail)
			}

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Internal, s.Code())
			require.Equal(t, tc.expectedMessage, s.Message())
			testhelper.ProtoEqual(t, anyDetails, s.Details())
		})
	}
}
