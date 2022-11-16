//go:build !gitaly_test_sha256

package blob

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

func TestGetBlob_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setup(t, ctx)

	maintenanceMdBlobData := testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt")

	for _, tc := range []struct {
		desc            string
		oid             string
		limit           int64
		expectedContent []byte
		expectedSize    int64
	}{
		{
			desc:            "unlimited fetch",
			oid:             "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:           -1,
			expectedContent: maintenanceMdBlobData,
			expectedSize:    int64(len(maintenanceMdBlobData)),
		},
		{
			desc:            "limit larger than blob size",
			oid:             "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:           int64(len(maintenanceMdBlobData) + 1),
			expectedContent: maintenanceMdBlobData,
			expectedSize:    int64(len(maintenanceMdBlobData)),
		},
		{
			desc:         "limit zero",
			oid:          "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:        0,
			expectedSize: int64(len(maintenanceMdBlobData)),
		},
		{
			desc:            "limit greater than zero, less than blob size",
			oid:             "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:           10,
			expectedContent: maintenanceMdBlobData[:10],
			expectedSize:    int64(len(maintenanceMdBlobData)),
		},
		{
			desc:            "large blob",
			oid:             "08cf843fd8fe1c50757df0a13fcc44661996b4df",
			limit:           10,
			expectedContent: []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46},
			expectedSize:    111803,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        tc.oid,
				Limit:      tc.limit,
			})
			require.NoError(t, err, "initiate RPC")

			reportedSize, reportedOid, data, err := getBlob(stream)
			require.NoError(t, err, "consume response")

			require.Equal(t, tc.expectedSize, reportedSize, "real blob size")

			require.NotEmpty(t, reportedOid)
			require.True(t, bytes.Equal(tc.expectedContent, data), "returned data exactly as expected")
		})
	}
}

func TestGetBlob_notFound(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setup(t, ctx)

	stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
		Repository: repo,
		Oid:        "doesnotexist",
	})
	require.NoError(t, err)

	reportedSize, reportedOid, data, err := getBlob(stream)
	require.NoError(t, err)
	require.Zero(t, reportedSize)
	require.Empty(t, reportedOid)
	require.Zero(t, len(data))
}

func getBlob(stream gitalypb.BlobService_GetBlobClient) (int64, string, []byte, error) {
	firstResponse, err := stream.Recv()
	if err != nil {
		return 0, "", nil, err
	}

	data := &bytes.Buffer{}
	_, err = data.Write(firstResponse.GetData())
	if err != nil {
		return 0, "", nil, err
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		if response.GetSize() != 0 {
			return nil, fmt.Errorf("size may only be set in the first response message")
		}
		if len(response.GetOid()) != 0 {
			return nil, fmt.Errorf("oid may only be set in the first response message")
		}
		return response.GetData(), err
	})

	_, err = io.Copy(data, reader)
	return firstResponse.Size, firstResponse.Oid, data.Bytes(), err
}

func TestGetBlob_invalidRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, client := setup(t, ctx)

	oid := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.GetBlobRequest
		expectedErr error
	}{
		{
			desc: "missing repository",
			request: &gitalypb.GetBlobRequest{
				Repository: nil,
				Oid:        oid,
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "invalid storage name",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: helper.ErrInvalidArgumentf(testhelper.GitalyOrPraefectMessage(
				fmt.Sprintf("create object reader: GetStorageByName: no such storage: %q", "fake"),
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "invalid relative path",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: helper.ErrNotFoundf(testhelper.GitalyOrPraefectMessage(
				fmt.Sprintf("create object reader: GetRepoPath: not a git repository: %q", filepath.Join(cfg.Storages[0].Path, "path")),
				fmt.Sprintf("accessor call: route repository accessor: consistent storages: repository %q/%q not found", cfg.Storages[0].Name, "path"),
			)),
		},
		{
			desc: "missing object ID",
			request: &gitalypb.GetBlobRequest{
				Repository: repo,
			},
			expectedErr: helper.ErrInvalidArgumentf("empty Oid"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, tc.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
