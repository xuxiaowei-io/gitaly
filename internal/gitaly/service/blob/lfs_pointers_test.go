//go:build !gitaly_test_sha256

package blob

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	lfsPointer1 = "0c304a93cb8430108629bbbcaa27db3343299bc0"
	lfsPointer2 = "f78df813119a79bfbe0442ab92540a61d3ab7ff3"
	lfsPointer3 = "bab31d249f78fba464d1b75799aad496cc07fa3b"
	lfsPointer4 = "125fcc9f6e33175cb278b9b2809154d2535fe19f"
	lfsPointer5 = "0360724a0d64498331888f1eaef2d24243809230"
	lfsPointer6 = "ff0ab3afd1616ff78d0331865d922df103b64cf0"
)

var lfsPointers = map[string]*gitalypb.LFSPointer{
	lfsPointer1: {
		Size: 133,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
		Oid:  lfsPointer1,
	},
	lfsPointer2: {
		Size: 127,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
		Oid:  lfsPointer2,
	},
	lfsPointer3: {
		Size: 127,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
		Oid:  lfsPointer3,
	},
	lfsPointer4: {
		Size: 129,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:47997ea7ecff33be61e3ca1cc287ee72a2125161518f1a169f2893a5a82e9d95\nsize 7501\n"),
		Oid:  lfsPointer4,
	},
	lfsPointer5: {
		Size: 129,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:8c1e8de917525f83104736f6c64d32f0e2a02f5bf2ee57843a54f222cba8c813\nsize 2797\n"),
		Oid:  lfsPointer5,
	},
	lfsPointer6: {
		Size: 132,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:96f74c6fe7a2979eefb9ec74a5dfc6888fb25543cf99b77586b79afea1da6f97\nsize 1219696\n"),
		Oid:  lfsPointer6,
	},
}

func TestListLFSPointers(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, client := setupWithoutRepo(t, ctx)
	repo, _, repoInfo := setupRepoWithLFS(t, ctx, cfg)

	ctx = testhelper.MergeOutgoingMetadata(ctx,
		metadata.Pairs(catfile.SessionIDField, "1"),
	)

	for _, tc := range []struct {
		desc             string
		revs             []string
		limit            int32
		expectedPointers []*gitalypb.LFSPointer
		expectedErr      error
	}{
		{
			desc:        "missing revisions",
			revs:        []string{},
			expectedErr: status.Error(codes.InvalidArgument, "missing revisions"),
		},
		{
			desc:        "invalid revision",
			revs:        []string{"-dashed"},
			expectedErr: status.Error(codes.InvalidArgument, "invalid revision: \"-dashed\""),
		},
		{
			desc: "object IDs",
			revs: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				repoInfo.defaultTreeID.String(),   // tree
				repoInfo.defaultCommitID.String(), // commit
			},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
		},
		{
			desc: "revision",
			revs: []string{"refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
			},
		},
		{
			desc: "pseudo-revisions",
			revs: []string{"refs/heads/master", "--not", "--all"},
		},
		{
			desc: "partial graph walk",
			revs: []string{"--all", "--not", "refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "partial graph walk with matching limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 5,
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "partial graph walk with limiting limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 3,
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListLFSPointers(ctx, &gitalypb.ListLFSPointersRequest{
				Repository: repo,
				Revisions:  tc.revs,
				Limit:      tc.limit,
			})
			require.NoError(t, err)

			var actualLFSPointers []*gitalypb.LFSPointer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				if err != nil {
					break
				}

				actualLFSPointers = append(actualLFSPointers, resp.GetLfsPointers()...)
			}
			lfsPointersEqual(t, tc.expectedPointers, actualLFSPointers)
		})
	}
}

func TestListAllLFSPointers(t *testing.T) {
	ctx := testhelper.Context(t)

	receivePointers := func(t *testing.T, stream gitalypb.BlobService_ListAllLFSPointersClient) []*gitalypb.LFSPointer {
		t.Helper()

		var pointers []*gitalypb.LFSPointer
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.Nil(t, err)
			pointers = append(pointers, resp.GetLfsPointers()...)
		}
		return pointers
	}

	lfsPointerContents := `version https://git-lfs.github.com/spec/v1
oid sha256:1111111111111111111111111111111111111111111111111111111111111111
size 12345`

	t.Run("normal repository", func(t *testing.T) {
		_, repo, _, client := setupWithLFS(t, ctx)
		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			lfsPointers[lfsPointer1],
			lfsPointers[lfsPointer2],
			lfsPointers[lfsPointer3],
			lfsPointers[lfsPointer4],
			lfsPointers[lfsPointer5],
			lfsPointers[lfsPointer6],
		}, receivePointers(t, stream))
	})

	t.Run("dangling LFS pointer", func(t *testing.T) {
		cfg, repo, repoPath, client := setupWithLFS(t, ctx)

		hash := gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: strings.NewReader(lfsPointerContents)},
			"-C", repoPath, "hash-object", "-w", "--stdin",
		)
		lfsPointerOID := text.ChompBytes(hash)

		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			{
				Oid:  lfsPointerOID,
				Data: []byte(lfsPointerContents),
				Size: int64(len(lfsPointerContents)),
			},
			lfsPointers[lfsPointer1],
			lfsPointers[lfsPointer2],
			lfsPointers[lfsPointer3],
			lfsPointers[lfsPointer4],
			lfsPointers[lfsPointer5],
			lfsPointers[lfsPointer6],
		}, receivePointers(t, stream))
	})

	t.Run("quarantine", func(t *testing.T) {
		cfg, repoProto, repoPath, client := setupWithLFS(t, ctx)

		// We're emulating the case where git is receiving data via a push, where objects
		// are stored in a separate quarantine environment. In this case, LFS pointer checks
		// may want to inspect all newly pushed objects, denoted by a repository proto
		// message which only has its object directory set to the quarantine directory.
		quarantineDir := "objects/incoming-123456"
		require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), perm.PublicDir))
		repoProto.GitObjectDirectory = quarantineDir
		repoProto.GitAlternateObjectDirectories = nil

		// There are no quarantined objects yet, so none should be returned here.
		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repoProto,
		})
		require.NoError(t, err)
		require.Empty(t, receivePointers(t, stream))

		// Write a new object into the repository. Because we set GIT_OBJECT_DIRECTORY to
		// the quarantine directory, objects will be written in there instead of into the
		// repository's normal object directory.
		repo := localrepo.NewTestRepo(t, cfg, repoProto)
		var buffer, stderr bytes.Buffer
		err = repo.ExecAndWait(ctx, git.Command{
			Name: "hash-object",
			Flags: []git.Option{
				git.Flag{Name: "-w"},
				git.Flag{Name: "--stdin"},
			},
		}, git.WithStdin(strings.NewReader(lfsPointerContents)), git.WithStdout(&buffer), git.WithStderr(&stderr))
		require.NoError(t, err)

		stream, err = client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repoProto,
		})
		require.NoError(t, err)

		// We only expect to find a single LFS pointer, which is the one we've just written
		// into the quarantine directory.
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			{
				Oid:  text.ChompBytes(buffer.Bytes()),
				Data: []byte(lfsPointerContents),
				Size: int64(len(lfsPointerContents)),
			},
		}, receivePointers(t, stream))
	})

	t.Run("no repository provided", func(t *testing.T) {
		_, _, _, client := setupWithLFS(t, ctx)
		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: nil,
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
			"empty Repository",
			"repo scoped: empty Repository",
		)), err)
	})
}

func TestSuccessfulGetLFSPointersRequest(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, client := setupWithoutRepo(t, ctx)
	repo, _, repoInfo := setupRepoWithLFS(t, ctx, cfg)

	lfsPointerIds := []string{
		lfsPointer1,
		lfsPointer2,
		lfsPointer3,
	}
	otherObjectIds := []string{
		repoInfo.defaultTreeID.String(),   // tree
		repoInfo.defaultCommitID.String(), // commit
	}

	expectedLFSPointers := []*gitalypb.LFSPointer{
		lfsPointers[lfsPointer1],
		lfsPointers[lfsPointer2],
		lfsPointers[lfsPointer3],
	}

	request := &gitalypb.GetLFSPointersRequest{
		Repository: repo,
		BlobIds:    append(lfsPointerIds, otherObjectIds...),
	}

	stream, err := client.GetLFSPointers(ctx, request)
	require.NoError(t, err)

	var receivedLFSPointers []*gitalypb.LFSPointer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		receivedLFSPointers = append(receivedLFSPointers, resp.GetLfsPointers()...)
	}

	lfsPointersEqual(t, receivedLFSPointers, expectedLFSPointers)
}

func TestFailedGetLFSPointersRequestDueToValidations(t *testing.T) {
	ctx := testhelper.Context(t)

	_, repo, _, client := setupWithLFS(t, ctx)

	testCases := []struct {
		desc    string
		request *gitalypb.GetLFSPointersRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: nil,
				BlobIds:    []string{"f00"},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty BlobIds",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: repo,
				BlobIds:    nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.GetLFSPointers(ctx, testCase.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcCode(t, err, testCase.code)
		})
	}
}

func lfsPointersEqual(tb testing.TB, expected, actual []*gitalypb.LFSPointer) {
	tb.Helper()

	for _, slice := range [][]*gitalypb.LFSPointer{expected, actual} {
		sort.Slice(slice, func(i, j int) bool {
			return strings.Compare(slice[i].Oid, slice[j].Oid) < 0
		})
	}

	require.Equal(tb, len(expected), len(actual))
	for i := range expected {
		testhelper.ProtoEqual(tb, expected[i], actual[i])
	}
}

func setupWithLFS(tb testing.TB, ctx context.Context) (config.Cfg, *gitalypb.Repository, string, gitalypb.BlobServiceClient) {
	tb.Helper()

	cfg, client := setupWithoutRepo(tb, ctx)
	repo, repoPath, _ := setupRepoWithLFS(tb, ctx, cfg)

	return cfg, repo, repoPath, client
}

type lfsRepoInfo struct {
	// defaultCommitID is the object ID of the commit pointed to by the default branch.
	defaultCommitID git.ObjectID
	// defaultTreeID is the object ID of the tree pointed to by the default branch.
	defaultTreeID git.ObjectID
}

// setRepoWithLFS configures a git repository with LFS pointers to be used in
// testing. The commit OID and root tree OID of the default branch are returned
// for use with some tests.
func setupRepoWithLFS(tb testing.TB, ctx context.Context, cfg config.Cfg) (*gitalypb.Repository, string, lfsRepoInfo) {
	tb.Helper()

	repo, repoPath := gittest.CreateRepository(tb, ctx, cfg)

	masterTreeID := gittest.WriteTree(tb, cfg, repoPath, []gittest.TreeEntry{
		{Mode: "100644", Path: lfsPointer1, Content: string(lfsPointers[lfsPointer1].Data)},
	})
	masterCommitID := gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTree(masterTreeID),
		gittest.WithBranch("master"),
	)

	_ = gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: lfsPointer2, Content: string(lfsPointers[lfsPointer2].Data)},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointer3, Content: string(lfsPointers[lfsPointer3].Data)},
		),
		gittest.WithBranch("foo"),
	)

	_ = gittest.WriteCommit(tb, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: lfsPointer4, Content: string(lfsPointers[lfsPointer4].Data)},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointer5, Content: string(lfsPointers[lfsPointer5].Data)},
			gittest.TreeEntry{Mode: "100644", Path: lfsPointer6, Content: string(lfsPointers[lfsPointer6].Data)},
		),
		gittest.WithBranch("bar"),
	)

	return repo, repoPath, lfsRepoInfo{
		defaultCommitID: masterCommitID,
		defaultTreeID:   masterTreeID,
	}
}
