//go:build !gitaly_test_sha256

package commit

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestTreeEntry(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.TreeEntryRequest
		expectedErr       error
		expectedResponses []*gitalypb.TreeEntryResponse
	}{
		{
			desc: "missing repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Revision: []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72"),
				Path:     []byte("a/b/c"),
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "unset repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: nil,
				Revision:   []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72"),
				Path:       []byte("a/b/c"),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   nil,
				Path:       []byte("a/b/c"),
			},
			expectedErr: structerr.NewInvalidArgument("empty revision"),
		},
		{
			desc: "empty path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72"),
			},
			expectedErr: structerr.NewInvalidArgument("empty Path"),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("--output=/meow"),
				Path:       []byte("a/b/c"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "negative limit",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72"),
				Path:       []byte("a/b/c"),
				Limit:      -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative Limit"),
		},
		{
			desc: "negative maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72"),
				Path:       []byte("a/b/c"),
				MaxSize:    -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative MaxSize"),
		},
		{
			desc: "object size exceeds maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
				Path:       []byte("MAINTENANCE.md"),
				MaxSize:    10,
			},
			expectedErr: structerr.NewFailedPrecondition("object size (1367) is bigger than the maximum allowed size (10)"),
		},
		{
			desc: "path escapes repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
				// Git blows up on paths like this
				Path: []byte("../bar/.gitkeep"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "../bar/.gitkeep"),
		},
		{
			desc: "missing file with space in path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("deadfacedeadfacedeadfacedeadfacedeadface"),
				Path:       []byte("with space/README.md"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "with space/README.md"),
		},
		{
			desc: "missing file",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
				Path:       []byte("missing.rb"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "missing.rb"),
		},
		{
			desc: "without limits",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
				Path:       []byte("MAINTENANCE.md"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
					Size: 1367,
					Mode: 0o100644,
					Data: testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
				},
			},
		},
		{
			desc: "with limit exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
				Path:       []byte("MAINTENANCE.md"),
				Limit:      40 * 1024,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
					Size: 1367,
					Mode: 0o100644,
					Data: testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
				},
			},
		},
		{
			desc: "with maximum size exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
				Path:       []byte("MAINTENANCE.md"),
				MaxSize:    40 * 1024,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
					Size: 1367,
					Mode: 0o100644,
					Data: testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
				},
			},
		},
		{
			desc: "path with space",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e"),
				Path:       []byte("with space/README.md"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "8c3014aceae45386c3c026a7ea4a1f68660d51d6",
					Size: 36,
					Mode: 0o100644,
					Data: testhelper.MustReadFile(t, "testdata/with-space-readme-md-blob.txt"),
				},
			},
		},
		{
			desc: "with limit that truncates the response",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("372ab6950519549b14d220271ee2322caa44d4eb"),
				Path:       []byte("gitaly/file-with-multiple-chunks"),
				Limit:      30 * 1024,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
					Size: 42220,
					Mode: 0o100644,
					Data: testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-truncated-blob.txt"),
				},
			},
		},
		{
			desc: "with submodule",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
				Path:       []byte("gitlab-grack"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_COMMIT,
					Oid:  "645f6c4c82fd3f5e06f67134450a570b795e55a6",
					Mode: 0o160000,
				},
			},
		},
		{
			desc: "subdirectory without trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"),
				Path:       []byte("files/js"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  "31405c5ddef582c5a9b7a85230413ff90e2fe720",
					Size: 83,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "subdirectory with trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"),
				Path:       []byte("files/js/"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  "31405c5ddef582c5a9b7a85230413ff90e2fe720",
					Size: 83,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "blob in subdirectory",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
				Path:       []byte("foo/bar/.gitkeep"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
					Size: 0,
					Mode: 0o100644,
				},
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.TreeEntry(ctx, tc.request)
			require.NoError(t, err)

			var responses []*gitalypb.TreeEntryResponse
			for {
				var response *gitalypb.TreeEntryResponse

				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}

					break
				}

				responses = append(responses, response)
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponses, responses)
		})
	}
}
