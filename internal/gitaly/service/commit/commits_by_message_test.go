package commit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitsByMessage(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	treeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Mode: "100644", Path: "ruby", Content: "foo bar"},
	})

	commitWithFileID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(`Files, encoding and much more

Files, encoding and much more

Signed-off-by: John Doe <john@doe.com>`), gittest.WithTreeEntries(gittest.TreeEntry{
		OID:  treeID,
		Mode: "040000",
		Path: "files",
	}), gittest.WithBranch("few-commits"))
	commit10ID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(`Commit #10

Commit #10`), gittest.WithBranch("few-commits"), gittest.WithParents(commitWithFileID))
	commit1ID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(`Commit #1

Commit #1`), gittest.WithBranch("few-commits"), gittest.WithParents(commit10ID))

	localrepo := localrepo.NewTestRepo(t, cfg, repo)
	commitWithFile, err := localrepo.ReadCommit(ctx, git.Revision(commitWithFileID))
	require.NoError(t, err)
	commit10, err := localrepo.ReadCommit(ctx, git.Revision(commit10ID))
	require.NoError(t, err)
	commit1, err := localrepo.ReadCommit(ctx, git.Revision(commit1ID))
	require.NoError(t, err)

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.CommitsByMessageRequest
		expectedCommits []*gitalypb.GitCommit
		expectedErr     error
	}{
		{
			desc: "revision and query",
			request: &gitalypb.CommitsByMessageRequest{
				Revision:   []byte("few-commits"),
				Query:      "commit #1",
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{commit1, commit10},
		},
		{
			desc: "revision, query and limit",
			request: &gitalypb.CommitsByMessageRequest{
				Revision:   []byte("few-commits"),
				Query:      "commit #1",
				Limit:      1,
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{commit1},
		},
		{
			desc: "revision, query and offset",
			request: &gitalypb.CommitsByMessageRequest{
				Revision:   []byte("few-commits"),
				Query:      "commit #1",
				Offset:     1,
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{commit10},
		},
		{
			desc: "query, empty revision and path",
			request: &gitalypb.CommitsByMessageRequest{
				Query:      "much more",
				Path:       []byte("files/ruby"),
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{commitWithFile},
		},
		{
			desc: "query, empty revision and wildcard pathspec",
			request: &gitalypb.CommitsByMessageRequest{
				Query:      "much more",
				Path:       []byte("files/*"),
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{commitWithFile},
		},
		{
			desc: "query, empty revision and non-existent literal pathspec",
			request: &gitalypb.CommitsByMessageRequest{
				Query:         "much more",
				Path:          []byte("files/*"),
				GlobalOptions: &gitalypb.GlobalOptions{LiteralPathspecs: true},
				Repository:    repo,
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "query, empty revision and path not in the commits",
			request: &gitalypb.CommitsByMessageRequest{
				Query:      "much more",
				Path:       []byte("bar"),
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "query and bad revision",
			request: &gitalypb.CommitsByMessageRequest{
				Revision:   []byte("maaaaasterrrrr"),
				Query:      "much more",
				Repository: repo,
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "invalid repository",
			request: &gitalypb.CommitsByMessageRequest{
				Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
				Query:      "foo",
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc:        "repository is nil",
			request:     &gitalypb.CommitsByMessageRequest{Query: "foo"},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc:        "query is missing",
			request:     &gitalypb.CommitsByMessageRequest{Repository: repo},
			expectedErr: status.Error(codes.InvalidArgument, "empty Query"),
		},
		{
			desc:        "revision is invalid",
			request:     &gitalypb.CommitsByMessageRequest{Repository: repo, Revision: []byte("--output=/meow"), Query: "not empty"},
			expectedErr: status.Error(codes.InvalidArgument, "revision can't start with '-'"),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			c, err := client.CommitsByMessage(ctx, tc.request)
			require.NoError(t, err)

			receivedCommits := getAllCommits(t, func() (gitCommitsGetter, error) {
				resp, err := c.Recv()
				if err != nil && err != io.EOF {
					testhelper.RequireGrpcError(t, tc.expectedErr, err)
					err = io.EOF
				}
				return resp, err
			})

			require.Equal(t, len(tc.expectedCommits), len(receivedCommits), "number of commits received")

			for i, receivedCommit := range receivedCommits {
				testhelper.ProtoEqual(t, tc.expectedCommits[i], receivedCommit)
			}
		})
	}
}
