package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestLastCommitForPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	initialCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Content: "unmodified", Mode: "100644"},
		gittest.TreeEntry{Path: "delete-me", Content: "something", Mode: "100644"},
	))
	deletedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(initialCommitID), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Content: "unmodified", Mode: "100644"},
	))
	modifiedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(deletedCommitID), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Content: "modified", Mode: "100644"},
	))
	globCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(modifiedCommitID), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Content: "modified", Mode: "100644"},
		gittest.TreeEntry{Path: ":wq", Content: "glob", Mode: "100644"},
	))
	latestCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(globCommitID), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Content: "modified", Mode: "100644"},
		gittest.TreeEntry{Path: ":wq", Content: "glob", Mode: "100644"},
		gittest.TreeEntry{Path: "uninteresting", Content: "uninteresting", Mode: "100644"},
	))

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	deletedCommit, err := repo.ReadCommit(ctx, deletedCommitID.Revision())
	require.NoError(t, err)
	modifiedCommit, err := repo.ReadCommit(ctx, modifiedCommitID.Revision())
	require.NoError(t, err)
	globCommit, err := repo.ReadCommit(ctx, globCommitID.Revision())
	require.NoError(t, err)
	latestCommit, err := repo.ReadCommit(ctx, latestCommitID.Revision())
	require.NoError(t, err)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.LastCommitForPathRequest
		expectedErr      error
		expectedResponse *gitalypb.LastCommitForPathResponse
	}{
		{
			desc: "invalid storage",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: gittest.NewRepositoryName(t),
				},
				Revision: []byte("some-branch"),
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "unset repository",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: nil,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "unset revision",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
			},
			expectedErr: structerr.NewInvalidArgument("empty revision"),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Path:       []byte("foo/bar"),
				Revision:   []byte("--output=/meow"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "path present",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte("file"),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: modifiedCommit,
			},
		},
		{
			desc: "path empty",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte(""),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: latestCommit,
			},
		},
		{
			desc: "path is '/'",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte("/"),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: latestCommit,
			},
		},
		{
			desc: "path is '*'",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte("*"),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: latestCommit,
			},
		},
		{
			desc: "deleted file",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte("delete-me"),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: deletedCommit,
			},
		},
		{
			desc: "missing file",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repoProto,
				Revision:   []byte(latestCommitID),
				Path:       []byte("does-not-exist"),
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: nil,
			},
		},
		{
			desc: "glob with literal pathspec",
			request: &gitalypb.LastCommitForPathRequest{
				Repository:      repoProto,
				Revision:        []byte(latestCommitID),
				Path:            []byte(":wq"),
				LiteralPathspec: true,
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: globCommit,
			},
		},
		{
			desc: "glob without literal pathspec",
			request: &gitalypb.LastCommitForPathRequest{
				Repository:      repoProto,
				Revision:        []byte(latestCommitID),
				Path:            []byte(":wq"),
				LiteralPathspec: false,
			},
			expectedResponse: &gitalypb.LastCommitForPathResponse{
				Commit: nil,
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.LastCommitForPath(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
