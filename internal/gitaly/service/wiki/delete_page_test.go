package wiki

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func testSuccessfulWikiDeletePageRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	wikiRepoProto, wikiRepoPath, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	wikiRepo := localrepo.NewTestRepo(t, cfg, wikiRepoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := setupWikiService(t, cfg, rubySrv)

	pageName := "A talé of two wikis"
	authorID := int32(1)
	authorUserName := []byte("ahmad")
	authorName := []byte("Ahmad Sherif")
	authorEmail := []byte("ahmad@gitlab.com")
	message := []byte("Delete " + pageName)
	content := []byte("It was the best of wikis, it was the worst of wikis")

	testCases := []struct {
		desc string
		req  *gitalypb.WikiDeletePageRequest
	}{
		{
			desc: "with user id and username",
			req: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepoProto,
				PagePath:   []byte("a-talé-of-two-wikis"),
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     authorName,
					Email:    authorEmail,
					Message:  message,
					UserId:   authorID,
					UserName: authorUserName,
				},
			},
		},
		{
			desc: "without user id and username", // deprecate in GitLab 11.0 https://gitlab.com/gitlab-org/gitaly/issues/1154
			req: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepoProto,
				PagePath:   []byte("a-talé-of-two-wikis"),
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:    authorName,
					Email:   authorEmail,
					Message: message,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			writeWikiPage(t, client, wikiRepoProto, createWikiPageOpts{title: pageName, content: content})

			_, err := client.WikiDeletePage(ctx, tc.req)
			require.NoError(t, err)

			headID := gittest.Exec(t, cfg, "-C", wikiRepoPath, "show", "--format=format:%H", "--no-patch", "HEAD")
			commit, err := wikiRepo.ReadCommit(ctx, git.Revision(headID))
			require.NoError(t, err, "look up git commit after deleting a wiki page")

			require.Equal(t, authorName, commit.Author.Name, "author name mismatched")
			require.Equal(t, authorEmail, commit.Author.Email, "author email mismatched")
			require.Equal(t, message, commit.Subject, "message mismatched")
		})
	}
}

func testFailedWikiDeletePageDueToValidations(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	client := setupWikiService(t, cfg, rubySrv)

	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Delete a wiki page"),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	testCases := []struct {
		desc    string
		request *gitalypb.WikiDeletePageRequest
		code    codes.Code
	}{
		{
			desc: "non existent  page path",
			request: &gitalypb.WikiDeletePageRequest{
				Repository:    wikiRepo,
				PagePath:      []byte("does-not-exist"),
				CommitDetails: commitDetails,
			},
			code: codes.NotFound,
		},
		{
			desc: "empty page path",
			request: &gitalypb.WikiDeletePageRequest{
				Repository:    wikiRepo,
				CommitDetails: commitDetails,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details",
			request: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepo,
				PagePath:   []byte("does-not-matter"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' name",
			request: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepo,
				PagePath:   []byte("does-not-matter"),
				CommitDetails: &gitalypb.WikiCommitDetails{
					Email:    []byte("a@b.com"),
					Message:  []byte("A message"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' email",
			request: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepo,
				PagePath:   []byte("does-not-matter"),
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     []byte("A name"),
					Message:  []byte("A message"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' message",
			request: &gitalypb.WikiDeletePageRequest{
				Repository: wikiRepo,
				PagePath:   []byte("does-not-matter"),
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     []byte("A name"),
					Email:    []byte("a@b.com"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.WikiDeletePage(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}