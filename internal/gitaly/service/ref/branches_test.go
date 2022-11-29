//go:build !gitaly_test_sha256

package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulFindBranchRequest(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, repoProto, _, client := setupRefService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	branchesByName := make(map[git.ReferenceName]*gitalypb.Branch)
	for branchName, revision := range map[git.ReferenceName]git.Revision{
		"refs/heads/branch":            "refs/heads/master~0",
		"refs/heads/heads/branch":      "refs/heads/master~1",
		"refs/heads/refs/heads/branch": "refs/heads/master~2",
	} {
		oid, err := repo.ResolveRevision(ctx, revision)
		require.NoError(t, err)

		err = repo.UpdateRef(ctx, branchName, oid, "")
		require.NoError(t, err)

		commit, err := repo.ReadCommit(ctx, branchName.Revision())
		require.NoError(t, err)

		branchesByName[branchName] = &gitalypb.Branch{
			Name:         []byte(branchName.String()[len("refs/heads/"):]),
			TargetCommit: commit,
		}
	}

	testCases := []struct {
		desc           string
		branchName     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:           "regular branch name",
			branchName:     "branch",
			expectedBranch: branchesByName["refs/heads/branch"],
		},
		{
			desc:           "absolute reference path",
			branchName:     "heads/branch",
			expectedBranch: branchesByName["refs/heads/heads/branch"],
		},
		{
			desc:           "heads path",
			branchName:     "refs/heads/branch",
			expectedBranch: branchesByName["refs/heads/refs/heads/branch"],
		},
		{
			desc:       "non-existent branch",
			branchName: "i-do-not-exist-on-this-repo",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: repoProto,
				Name:       []byte(testCase.branchName),
			}

			response, err := client.FindBranch(ctx, request)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch, "mismatched branches")
		})
	}
}

func TestFailedFindBranchRequest(t *testing.T) {
	ctx := testhelper.Context(t)
	_, repo, _, client := setupRefService(t, ctx)

	testCases := []struct {
		desc        string
		repo        *gitalypb.Repository
		branchName  string
		expectedErr error
	}{
		{
			desc: "no repository provided",
			repo: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "empty branch name",
			repo:        repo,
			branchName:  "",
			expectedErr: status.Error(codes.InvalidArgument, "Branch name cannot be empty"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: testCase.repo,
				Name:       []byte(testCase.branchName),
			}

			_, err := client.FindBranch(ctx, request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}
