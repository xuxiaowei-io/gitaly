//go:build !gitaly_test_sha256

package ref

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestSuccessfulFindLocalBranches(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRefService(t, ctx)

	rpcRequest := &gitalypb.FindLocalBranchesRequest{Repository: repo}
	c, err := client.FindLocalBranches(ctx, rpcRequest)
	require.NoError(t, err)

	var branches []*gitalypb.Branch
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		branches = append(branches, r.GetLocalBranches()...)
	}

	for name, target := range localBranches {
		localBranch := &gitalypb.Branch{
			Name:         []byte(name),
			TargetCommit: target,
		}

		assertContainsBranch(t, branches, localBranch)
	}
}

func TestFindLocalBranchesHugeCommitter(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, repoPath, client := setupRefService(t, ctx)

	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("refs/heads/improve/awesome"),
		gittest.WithCommitterName(strings.Repeat("A", 100000)),
	)

	rpcRequest := &gitalypb.FindLocalBranchesRequest{Repository: repo}

	c, err := client.FindLocalBranches(ctx, rpcRequest)
	require.NoError(t, err)

	for {
		_, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
}

func TestFindLocalBranchesPagination(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRefService(t, ctx)

	limit := 1
	rpcRequest := &gitalypb.FindLocalBranchesRequest{
		Repository: repo,
		PaginationParams: &gitalypb.PaginationParameter{
			Limit:     int32(limit),
			PageToken: "refs/heads/gitaly/squash-test",
		},
	}
	c, err := client.FindLocalBranches(ctx, rpcRequest)
	require.NoError(t, err)

	expectedBranch := "refs/heads/improve/awesome"
	target := localBranches[expectedBranch]

	var branches []*gitalypb.Branch
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		branches = append(branches, r.GetLocalBranches()...)
	}

	require.Len(t, branches, limit)

	branch := &gitalypb.Branch{
		Name:         []byte(expectedBranch),
		TargetCommit: target,
	}
	assertContainsBranch(t, branches, branch)
}

func TestFindLocalBranchesPaginationSequence(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRefService(t, ctx)

	limit := 2
	firstRPCRequest := &gitalypb.FindLocalBranchesRequest{
		Repository: repo,
		PaginationParams: &gitalypb.PaginationParameter{
			Limit: int32(limit),
		},
	}
	c, err := client.FindLocalBranches(ctx, firstRPCRequest)
	require.NoError(t, err)

	var firstResponseBranches []*gitalypb.Branch
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		firstResponseBranches = append(firstResponseBranches, r.GetLocalBranches()...)
	}

	require.Len(t, firstResponseBranches, limit)

	secondRPCRequest := &gitalypb.FindLocalBranchesRequest{
		Repository: repo,
		PaginationParams: &gitalypb.PaginationParameter{
			Limit:     1,
			PageToken: string(firstResponseBranches[0].Name),
		},
	}
	c, err = client.FindLocalBranches(ctx, secondRPCRequest)
	require.NoError(t, err)

	var secondResponseBranches []*gitalypb.Branch
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		secondResponseBranches = append(secondResponseBranches, r.GetLocalBranches()...)
	}

	require.Len(t, secondResponseBranches, 1)
	require.Equal(t, firstResponseBranches[1], secondResponseBranches[0])
}

func TestFindLocalBranchesPaginationWithIncorrectToken(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRefService(t, ctx)

	limit := 1
	rpcRequest := &gitalypb.FindLocalBranchesRequest{
		Repository: repo,
		PaginationParams: &gitalypb.PaginationParameter{
			Limit:     int32(limit),
			PageToken: "refs/heads/random-unknown-branch",
		},
	}
	c, err := client.FindLocalBranches(ctx, rpcRequest)
	require.NoError(t, err)

	_, err = c.Recv()
	require.NotEqual(t, err, io.EOF)
	testhelper.RequireGrpcError(t, structerr.NewInternal("finding refs: could not find page token"), err)
}

// Test that `s` contains the elements in `relativeOrder` in that order
// (relative to each other)
func isOrderedSubset(subset, set []string) bool {
	subsetIndex := 0 // The string we are currently looking for from `subset`
	for _, element := range set {
		if element != subset[subsetIndex] {
			continue
		}

		subsetIndex++

		if subsetIndex == len(subset) { // We found all elements in that order
			return true
		}
	}
	return false
}

func TestFindLocalBranchesSort(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	testCases := []struct {
		desc          string
		relativeOrder []string
		sortBy        gitalypb.FindLocalBranchesRequest_SortBy
	}{
		{
			desc:          "In ascending order by name",
			relativeOrder: []string{"refs/heads/'test'", "refs/heads/100%branch", "refs/heads/improve/awesome", "refs/heads/master"},
			sortBy:        gitalypb.FindLocalBranchesRequest_NAME,
		},
		{
			desc:          "In ascending order by commiter date",
			relativeOrder: []string{"refs/heads/improve/awesome", "refs/heads/'test'", "refs/heads/100%branch", "refs/heads/master"},
			sortBy:        gitalypb.FindLocalBranchesRequest_UPDATED_ASC,
		},
		{
			desc:          "In descending order by commiter date",
			relativeOrder: []string{"refs/heads/master", "refs/heads/100%branch", "refs/heads/'test'", "refs/heads/improve/awesome"},
			sortBy:        gitalypb.FindLocalBranchesRequest_UPDATED_DESC,
		},
	}

	_, repo, _, client := setupRefService(t, ctx)

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			rpcRequest := &gitalypb.FindLocalBranchesRequest{Repository: repo, SortBy: testCase.sortBy}

			c, err := client.FindLocalBranches(ctx, rpcRequest)
			require.NoError(t, err)

			var branches []string
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				for _, branch := range r.GetLocalBranches() {
					branches = append(branches, string(branch.Name))
				}
			}

			if !isOrderedSubset(testCase.relativeOrder, branches) {
				t.Fatalf("%s: Expected branches to have relative order %v; got them as %v", testCase.desc, testCase.relativeOrder, branches)
			}
		})
	}
}

func TestFindLocalBranches_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, _, client := setupRefService(t, ctx)
	for _, tc := range []struct {
		desc        string
		repo        *gitalypb.Repository
		expectedErr error
	}{
		{
			desc:        "repository not provided",
			repo:        nil,
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "repository doesn't exist on disk",
			repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "made/up/path"},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "made/up/path")),
			),
		},
		{
			desc: "unknown storage",
			repo: &gitalypb.Repository{StorageName: "invalid", RelativePath: repo.GetRelativePath()},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("invalid"),
			)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.FindLocalBranches(ctx, &gitalypb.FindLocalBranchesRequest{Repository: tc.repo})
			require.NoError(t, err)
			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
