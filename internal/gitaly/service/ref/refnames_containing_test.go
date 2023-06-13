package ref

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestListTagNamesContainingCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefServiceWithoutRepo(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	commitA := gittest.WriteCommit(t, cfg, repoPath)
	commitB := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commitA))
	commitC := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commitB), gittest.WithBranch(git.DefaultBranch))

	gittest.WriteTag(t, cfg, repoPath, "annotated", commitA.Revision(), gittest.WriteTagConfig{
		Message: "annotated",
	})
	gittest.WriteTag(t, cfg, repoPath, "lightweight", commitB.Revision())

	for _, tc := range []struct {
		desc         string
		request      *gitalypb.ListTagNamesContainingCommitRequest
		expectedErr  error
		expectedTags []string
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: nil,
			},
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				structerr.NewInvalidArgument("repo scoped: %w", storage.ErrRepositoryNotSet),
			),
		},
		{
			desc: "invalid commit ID",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "invalid",
			},
			expectedErr: structerr.NewInvalidArgument(
				fmt.Sprintf(`invalid object ID: "invalid", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen()),
			),
		},
		{
			desc: "no commit ID",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "",
			},
			expectedErr: structerr.NewInvalidArgument(
				`invalid object ID: "", expected length %d, got 0`, gittest.DefaultObjectHash.EncodedLen(),
			),
		},
		{
			desc: "commit not contained in any tag",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitC.String(),
			},
			expectedTags: nil,
		},
		{
			desc: "root commit",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitA.String(),
			},
			expectedTags: []string{"annotated", "lightweight"},
		},
		{
			desc: "root commit with limit",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitA.String(),
				Limit:      1,
			},
			expectedTags: []string{"annotated"},
		},
		{
			desc: "commit with single tag",
			request: &gitalypb.ListTagNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   commitB.String(),
			},
			expectedTags: []string{"lightweight"},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListTagNamesContainingCommit(ctx, tc.request)
			require.NoError(t, err)

			var tagNames []string
			for {
				var response *gitalypb.ListTagNamesContainingCommitResponse
				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}

					break
				}

				for _, tagName := range response.GetTagNames() {
					tagNames = append(tagNames, string(tagName))
				}
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.ElementsMatch(t, tc.expectedTags, tagNames)
		})
	}
}

func TestListBranchNamesContainingCommit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRefServiceWithoutRepo(t)
	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	rootCommitID := gittest.WriteCommit(t, cfg, repoPath)
	intermediateCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(rootCommitID))
	headCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(intermediateCommitID), gittest.WithBranch(git.DefaultBranch))
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(intermediateCommitID), gittest.WithBranch("branch"))

	ambiguousCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("ambiguous"), gittest.WithParents(intermediateCommitID))
	gittest.WriteRef(t, cfg, repoPath, git.ReferenceName("refs/heads/"+ambiguousCommitID.String()), ambiguousCommitID)

	unreferencedCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreferenced"))

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.ListBranchNamesContainingCommitRequest
		expectedErr      error
		expectedBranches []string
	}{
		{
			desc: "repository not provided",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: nil,
			},
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				structerr.NewInvalidArgument("repo scoped: %w", storage.ErrRepositoryNotSet),
			),
		},
		{
			desc: "invalid commit",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "invalid",
			},
			expectedErr: structerr.NewInvalidArgument(
				fmt.Sprintf(`invalid object ID: "invalid", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen()),
			),
		},
		{
			desc: "no commit ID",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   "",
			},
			expectedErr: structerr.NewInvalidArgument(
				fmt.Sprintf(`invalid object ID: "", expected length %v, got 0`, gittest.DefaultObjectHash.EncodedLen()),
			),
		},
		{
			desc: "current HEAD",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   headCommitID.String(),
			},
			expectedBranches: []string{git.DefaultBranch, "branch"},
		},
		{
			desc: "branch name is also commit id",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   ambiguousCommitID.String(),
			},
			expectedBranches: []string{ambiguousCommitID.String()},
		},
		{
			desc: "initial commit",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   rootCommitID.String(),
			},
			expectedBranches: []string{
				git.DefaultBranch,
				"branch",
				ambiguousCommitID.String(),
			},
		},
		{
			desc: "commit without references",
			request: &gitalypb.ListBranchNamesContainingCommitRequest{
				Repository: repo,
				CommitId:   unreferencedCommitID.String(),
			},
			expectedBranches: nil,
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.ListBranchNamesContainingCommit(ctx, tc.request)
			require.NoError(t, err)

			var branchNames []string
			for {
				var response *gitalypb.ListBranchNamesContainingCommitResponse
				response, err = stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}

					break
				}

				for _, branchName := range response.GetBranchNames() {
					branchNames = append(branchNames, string(branchName))
				}
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			require.ElementsMatch(t, tc.expectedBranches, branchNames)
		})
	}
}
