//go:build !gitaly_test_sha256

package ref

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
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
	_, repo, _, client := setupRefService(t, ctx)

	testCases := []struct {
		description string
		commitID    string
		code        codes.Code
		limit       uint32
		branches    []string
	}{
		{
			description: "no commit ID",
			commitID:    "",
			code:        codes.InvalidArgument,
		},
		{
			description: "current master HEAD",
			commitID:    "e63f41fe459e62e1228fcef60d7189127aeba95a",
			code:        codes.OK,
			branches:    []string{"master"},
		},
		{
			// gitlab-test contains a branch refs/heads/1942eed5cc108b19c7405106e81fa96125d0be22
			// which is in conflict with a commit with the same ID
			description: "branch name is also commit id",
			commitID:    "1942eed5cc108b19c7405106e81fa96125d0be22",
			code:        codes.OK,
			branches:    []string{"1942eed5cc108b19c7405106e81fa96125d0be22"},
		},
		{
			description: "init commit",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			branches: []string{
				"deleted-image-test",
				"ends-with.json",
				"master",
				"conflict-non-utf8",
				"'test'",
				"ʕ•ᴥ•ʔ",
				"'test'",
				"100%branch",
			},
		},
		{
			description: "init commit",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			limit:       1,
			branches:    []string{"'test'"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			request := &gitalypb.ListBranchNamesContainingCommitRequest{Repository: repo, CommitId: tc.commitID}

			c, err := client.ListBranchNamesContainingCommit(ctx, request)
			require.NoError(t, err)

			var names []string
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				} else if tc.code != codes.OK {
					testhelper.RequireGrpcCode(t, err, tc.code)

					return
				}
				require.NoError(t, err)

				for _, name := range r.GetBranchNames() {
					names = append(names, string(name))
				}
			}

			// Test for inclusion instead of equality because new refs
			// will get added to the gitlab-test repo over time.
			require.Subset(t, names, tc.branches)
		})
	}
}

func TestListBranchNamesContainingCommit_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, repoProto, _, client := setupRefService(t, ctx)

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.ListBranchNamesContainingCommitRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.ListBranchNamesContainingCommitRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "bad commit provided",
			req:         &gitalypb.ListBranchNamesContainingCommitRequest{Repository: repoProto, CommitId: "invalid"},
			expectedErr: status.Error(codes.InvalidArgument, fmt.Sprintf(`invalid object ID: "invalid", expected length %v, got 7`, gittest.DefaultObjectHash.EncodedLen())),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListBranchNamesContainingCommit(ctx, tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
