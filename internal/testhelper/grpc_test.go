package testhelper_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"google.golang.org/grpc"
)

func TestSetCtxGrpcMethod(t *testing.T) {
	expectFullMethodName := "/pinkypb/TakeOverTheWorld.SNARF"
	ctx := testhelper.Context(t)

	ctx = testhelper.SetCtxGrpcMethod(ctx, expectFullMethodName)

	actualFullMethodName, ok := grpc.Method(ctx)
	require.True(t, ok, "expected context to contain server transport stream")
	require.Equal(t, expectFullMethodName, actualFullMethodName)
}
