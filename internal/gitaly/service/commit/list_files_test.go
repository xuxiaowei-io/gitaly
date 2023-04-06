//go:build !gitaly_test_sha256

package commit

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestListFiles(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", Content: "content"},
		gittest.TreeEntry{Path: "binary", Mode: "100644", Content: "\000something"},
		gittest.TreeEntry{Path: "subdir", Mode: "040000", OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
			{Path: "subfile", Mode: "100644", Content: "subcontent"},
		})},
	))
	defaultPaths := [][]byte{
		[]byte("file"),
		[]byte("binary"),
		[]byte("subdir/subfile"),
	}

	differentCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "different", Mode: "100644", Content: "different"},
	))

	for _, tc := range []struct {
		desc          string
		request       *gitalypb.ListFilesRequest
		expectedPaths [][]byte
		expectedErr   error
	}{
		{
			desc: "nil repo",
			request: &gitalypb.ListFilesRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty repo object",
			request: &gitalypb.ListFilesRequest{
				Repository: &gitalypb.Repository{},
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty StorageName",
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "non-existing repo",
			request: &gitalypb.ListFilesRequest{
				Repository: &gitalypb.Repository{StorageName: "foo", RelativePath: "bar"},
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				`GetStorageByName: no such storage: "foo"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("--output=/meow"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "valid object ID",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(commitID),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "HEAD",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("HEAD"),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "valid branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(git.DefaultBranch),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "valid fully qualified branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(git.DefaultRef),
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "non-default branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte(differentCommitID),
			},
			expectedPaths: [][]byte{[]byte("different")},
		},
		{
			desc: "missing object ID uses default branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte{},
			},
			expectedPaths: defaultPaths,
		},
		{
			desc: "invalid object ID",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("1234123412341234"),
			},
			expectedPaths: [][]byte{},
		},
		{
			desc: "invalid branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("non/existing"),
			},
			expectedPaths: [][]byte{},
		},
		{
			desc: "nonexisting fully qualified branch",
			request: &gitalypb.ListFilesRequest{
				Repository: repoProto,
				Revision:   []byte("refs/heads/foobar"),
			},
			expectedPaths: [][]byte{},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListFiles(ctx, tc.request)
			require.NoError(t, err)

			var files [][]byte
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						testhelper.RequireGrpcError(t, tc.expectedErr, err)
					}

					break
				}
				require.NoError(t, err)

				files = append(files, resp.GetPaths()...)
			}

			require.ElementsMatch(t, files, tc.expectedPaths)
		})
	}
}

func TestListFiles_unbornBranch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, _, client := setupCommitServiceWithRepo(t, ctx)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	tests := []struct {
		desc     string
		revision string
		code     codes.Code
	}{
		{
			desc:     "HEAD",
			revision: "HEAD",
		},
		{
			desc:     "unborn branch",
			revision: "refs/heads/master",
		},
		{
			desc:     "nonexisting branch",
			revision: "i-dont-exist",
		},
		{
			desc:     "nonexisting fully qualified branch",
			revision: "refs/heads/i-dont-exist",
		},
		{
			desc:     "missing revision without default branch",
			revision: "",
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "valid object ID",
			revision: "54fcc214b94e78d7a41a9a8fe6d87a5e59500e51",
		},
		{
			desc:     "invalid object ID",
			revision: "1234123412341234",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: repo, Revision: []byte(tc.revision),
			}

			c, err := client.ListFiles(ctx, &rpcRequest)
			require.NoError(t, err)

			var files [][]byte
			for {
				var resp *gitalypb.ListFilesResponse
				resp, err = c.Recv()
				if err != nil {
					break
				}

				require.NoError(t, err)
				files = append(files, resp.GetPaths()...)
			}

			if tc.code != codes.OK {
				testhelper.RequireGrpcCode(t, err, tc.code)
			} else {
				require.Equal(t, err, io.EOF)
			}
			require.Empty(t, files)
		})
	}
}
