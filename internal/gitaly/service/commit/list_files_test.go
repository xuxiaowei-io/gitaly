//go:build !gitaly_test_sha256

package commit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListFiles_success(t *testing.T) {
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
	}{
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
				if err == io.EOF {
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

func TestListFiles_failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, client := setupCommitService(t, ctx)

	tests := []struct {
		desc        string
		repo        *gitalypb.Repository
		expectedErr error
	}{
		{
			desc: "nil repo",
			repo: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty repo object",
			repo: &gitalypb.Repository{},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty StorageName",
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc: "non-existing repo",
			repo: &gitalypb.Repository{StorageName: "foo", RelativePath: "bar"},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`GetStorageByName: no such storage: "foo"`,
				"repo scoped: invalid Repository",
			)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: tc.repo, Revision: []byte("master"),
			}

			c, err := client.ListFiles(ctx, &rpcRequest)
			require.NoError(t, err)

			err = drainListFilesResponse(c)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func drainListFilesResponse(c gitalypb.CommitService_ListFilesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func TestListFiles_invalidRevision(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupCommitServiceWithRepo(t, ctx)

	stream, err := client.ListFiles(ctx, &gitalypb.ListFilesRequest{
		Repository: repo,
		Revision:   []byte("--output=/meow"),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
}
