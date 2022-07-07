package helper

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestError(t *testing.T) {
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPCCode := codes.Unauthenticated
	inputGRPC := status.Error(inputGRPCCode, errorMessage)

	for _, tc := range []struct {
		desc   string
		errorf func(err error) error
		code   codes.Code
	}{
		{
			desc:   "Canceled",
			errorf: ErrCanceled,
			code:   codes.Canceled,
		},
		{
			desc:   "Internal",
			errorf: ErrInternal,
			code:   codes.Internal,
		},
		{
			desc:   "InvalidArgument",
			errorf: ErrInvalidArgument,
			code:   codes.InvalidArgument,
		},
		{
			desc:   "FailedPrecondition",
			errorf: ErrFailedPrecondition,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "NotFound",
			errorf: ErrNotFound,
			code:   codes.NotFound,
		},
		{
			desc:   "Unavailable",
			errorf: ErrUnavailable,
			code:   codes.Unavailable,
		},
		{
			desc:   "AlreadyExists",
			errorf: ErrAlreadyExists,
			code:   codes.AlreadyExists,
		},
		{
			desc:   "Aborted",
			errorf: ErrAborted,
			code:   codes.Aborted,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// tc.code and our canary test code must not
			// clash!
			require.NotEqual(t, tc.code, inputGRPCCode)

			// When not re-throwing an error we get the
			// GRPC error code corresponding to the
			// function's name.
			err := tc.errorf(input)
			require.EqualError(t, err, errorMessage)
			require.False(t, errors.Is(err, inputGRPC))
			require.Equal(t, tc.code, status.Code(err))

			// When re-throwing an error an existing GRPC
			// error code will get preserved, instead of
			// the one corresponding to the function's
			// name.
			err = tc.errorf(inputGRPC)
			require.True(t, errors.Is(err, inputGRPC))
			require.False(t, errors.Is(err, input))
			require.Equal(t, inputGRPCCode, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))
		})
	}
}

func TestErrorF_withVFormat(t *testing.T) {
	testErrorfFormat(t, "top-level: %v", "top-level: %v")
}

func TestErrorF_withWFormat(t *testing.T) {
	testErrorfFormat(t, "top-level: %w", "top-level: %s")
}

func testErrorfFormat(t *testing.T, errorFormat, errorFormatEqual string) {
	isFormatW := strings.Contains(errorFormat, "%w")

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
			desc:         "ErrUnavailablef",
			errorf:       ErrUnavailablef,
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "ErrAbortedf",
			errorf:       ErrAbortedf,
			expectedCode: codes.Aborted,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("with non-gRPC error", func(t *testing.T) {
				err := tc.errorf(errorFormat, errors.New("nested"))
				require.EqualError(t, err, "top-level: nested")
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("with status.Errorf error", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, codes.Unauthenticated)

				err := tc.errorf(errorFormat, status.Errorf(codes.Unauthenticated, "deeply: %s", "nested"))
				require.EqualError(t, err, "top-level: rpc error: code = Unauthenticated desc = deeply: nested")
				if isFormatW {
					require.Equal(t, codes.Unauthenticated, status.Code(err))
				} else {
					require.Equal(t, tc.expectedCode, status.Code(err))
				}

				s, ok := status.FromError(err)
				require.True(t, ok)
				if isFormatW {
					require.Equal(t, status.New(codes.Unauthenticated, "top-level: rpc error: code = Unauthenticated desc = deeply: nested"), s)
				} else {
					require.Equal(t, status.New(tc.expectedCode, "top-level: rpc error: code = Unauthenticated desc = deeply: nested"), s)
				}
			})

			t.Run("with status.Error error", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, codes.Unauthenticated)

				err := tc.errorf(errorFormat, status.Error(codes.Unauthenticated, "nested"))
				require.EqualError(t, err, "top-level: rpc error: code = Unauthenticated desc = nested")
				if isFormatW {
					require.Equal(t, codes.Unauthenticated, status.Code(err))
				} else {
					require.Equal(t, tc.expectedCode, status.Code(err))
				}

				s, ok := status.FromError(err)
				require.True(t, ok)
				if isFormatW {
					require.Equal(t, status.New(codes.Unauthenticated, "top-level: rpc error: code = Unauthenticated desc = nested"), s)
				} else {
					require.Equal(t, status.New(tc.expectedCode, "top-level: rpc error: code = Unauthenticated desc = nested"), s)
				}
			})
		})
	}
}

func TestErrWithDetails(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		err             error
		details         []proto.Message
		expectedErr     error
		expectedMessage string
		expectedDetails []proto.Message
		expectedCode    codes.Code
	}{
		{
			desc:        "no error",
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "status with OK code",
			err:         status.Error(codes.OK, "message"),
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "normal error",
			err:         errors.New("message"),
			expectedErr: errors.New("error is not a gRPC status"),
		},
		{
			desc:            "status",
			err:             status.Error(codes.FailedPrecondition, "message"),
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
		},
		{
			desc: "status with details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
			},
		},
		{
			desc: "status with multiple details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
		},
		{
			desc: "status with mixed type details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			detailedErr, err := ErrWithDetails(tc.err, tc.details...)
			require.Equal(t, tc.expectedErr, err)
			if err != nil {
				return
			}

			require.Equal(t, tc.expectedMessage, detailedErr.Error())

			st, ok := status.FromError(detailedErr)
			require.True(t, ok, "error should be a status")
			require.Equal(t, tc.expectedCode, st.Code())

			statusProto := st.Proto()
			require.NotNil(t, statusProto)

			var details []proto.Message
			for _, detail := range statusProto.GetDetails() {
				detailProto, err := detail.UnmarshalNew()
				require.NoError(t, err)
				details = append(details, detailProto)
			}
			testhelper.ProtoEqual(t, tc.expectedDetails, details)
		})
	}
}
