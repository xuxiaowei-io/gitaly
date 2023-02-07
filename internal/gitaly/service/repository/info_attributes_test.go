//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetInfoAttributesExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	infoPath := filepath.Join(repoPath, "info")
	require.NoError(t, os.MkdirAll(infoPath, perm.SharedDir))

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := filepath.Join(infoPath, "attributes")
	err := os.WriteFile(attrsPath, data, perm.SharedFile)
	require.NoError(t, err)

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	stream, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	receivedData, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(t, err)
	require.Equal(t, data, receivedData)
}

func TestGetInfoAttributesNonExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(t, ctx)

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	response, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	message, err := response.Recv()
	require.NoError(t, err)

	require.Empty(t, message.GetAttributes())
}

func TestGetInfoAttributes_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, client := setupRepositoryServiceWithoutRepo(t)
	response, err := client.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{Repository: nil})
	require.NoError(t, err)
	_, err = response.Recv()
	msg := testhelper.GitalyOrPraefect("empty Repository", "repo scoped: empty Repository")
	testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, msg), err)
}
