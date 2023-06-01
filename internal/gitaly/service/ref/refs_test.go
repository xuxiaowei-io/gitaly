//go:build !gitaly_test_sha256

package ref

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFindDefaultBranchName(t *testing.T) {
	t.Parallel()

	type setupData struct {
		request     *gitalypb.FindDefaultBranchNameRequest
		expectedErr error
	}

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData
		expectedName git.ReferenceName
	}{
		{
			desc: "successful",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(git.DefaultRef.String()))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(git.LegacyDefaultRef.String()), gittest.WithParents(oid))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("apple"), gittest.WithParents(oid))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("banana"), gittest.WithParents(oid))

				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: repo,
					},
				}
			},
			expectedName: git.DefaultRef,
		},
		{
			desc: "successful, single branch",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("banana"))

				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: repo,
					},
				}
			},
			expectedName: "refs/heads/banana",
		},
		{
			desc: "successful, updated default",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(git.LegacyDefaultRef.String()))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("apple"), gittest.WithParents(oid))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("banana"), gittest.WithParents(oid))

				gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/banana")

				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: repo,
					},
				}
			},
			expectedName: "refs/heads/banana",
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: repo,
					},
				}
			},
		},
		{
			desc: "repository not provided",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{},
					expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
						"empty Repository",
						"repo scoped: empty Repository",
					)),
				}
			},
		},
		{
			desc: "repository doesn't exist on disk",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "made/up/path"},
					},
					expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefect(
						`GetRepoPath: not a git repository: "`+cfg.Storages[0].Path+`/made/up/path"`,
						`accessor call: route repository accessor: consistent storages: repository "default"/"made/up/path" not found`,
					)),
				}
			},
		},
		{
			desc: "unknown storage",
			setup: func(t *testing.T, ctx context.Context, cfg config.Cfg) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.FindDefaultBranchNameRequest{
						Repository: &gitalypb.Repository{StorageName: "invalid", RelativePath: repo.GetRelativePath()},
					},
					expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
						`GetStorageByName: no such storage: "invalid"`,
						"repo scoped: invalid Repository",
					)),
				}
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			cfg, client := setupRefServiceWithoutRepo(t)
			data := tc.setup(t, ctx, cfg)

			response, err := client.FindDefaultBranchName(ctx, data.request)
			testhelper.RequireGrpcError(t, data.expectedErr, err)
			require.Equal(t, tc.expectedName, git.ReferenceName(response.GetName()))
		})
	}
}

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
			desc: "repository not provided",
			repo: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "repository doesn't exist on disk",
			repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "made/up/path"},
			expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefect(
				`creating object reader: GetRepoPath: not a git repository: "`+cfg.Storages[0].Path+`/made/up/path"`,
				`accessor call: route repository accessor: consistent storages: repository "default"/"made/up/path" not found`,
			)),
		},
		{
			desc: "unknown storage",
			repo: &gitalypb.Repository{StorageName: "invalid", RelativePath: repo.GetRelativePath()},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`creating object reader: GetStorageByName: no such storage: "invalid"`,
				"repo scoped: invalid Repository",
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

func TestSuccessfulFindAllBranchesRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRefService(t, ctx)

	remoteBranch := &gitalypb.FindAllBranchesResponse_Branch{
		Name: []byte("refs/remotes/origin/fake-remote-branch"),
		Target: &gitalypb.GitCommit{
			Id:        "913c66a37b4a45b9769037c55c2d238bd0942d2e",
			Subject:   []byte("Files, encoding and much more"),
			Body:      []byte("Files, encoding and much more\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
			BodySize:  98,
			ParentIds: []string{"cfe32cf61b73a0d5e9f13e774abde7ff789b1660"},
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamppb.Timestamp{Seconds: 1393488896},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamppb.Timestamp{Seconds: 1393488896},
				Timezone: []byte("+0200"),
			},
			SignatureType: gitalypb.SignatureType_PGP,
			TreeId:        "faafbe7fe23fb83c664c78aaded9566c8f934412",
		},
	}

	gittest.WriteRef(t, cfg, repoPath, "refs/remotes/origin/fake-remote-branch", git.ObjectID(remoteBranch.Target.Id))

	request := &gitalypb.FindAllBranchesRequest{Repository: repo}
	c, err := client.FindAllBranches(ctx, request)
	require.NoError(t, err)

	branches := readFindAllBranchesResponsesFromClient(t, c)

	// It contains local branches
	for name, target := range localBranches {
		branch := &gitalypb.FindAllBranchesResponse_Branch{
			Name:   []byte(name),
			Target: target,
		}
		assertContainsAllBranchesResponseBranch(t, branches, branch)
	}

	// It contains our fake remote branch
	assertContainsAllBranchesResponseBranch(t, branches, remoteBranch)
}

func TestSuccessfulFindAllBranchesRequestWithMergedBranches(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, client := setupRefService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	localRefs := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--format=%(refname:strip=2)", "refs/heads")
	for _, ref := range strings.Split(string(localRefs), "\n") {
		ref = strings.TrimSpace(ref)
		if _, ok := localBranches["refs/heads/"+ref]; ok || ref == "master" || ref == "" {
			continue
		}
		gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", ref)
	}

	expectedRefs := []string{"refs/heads/100%branch", "refs/heads/improve/awesome", "refs/heads/'test'"}

	var expectedBranches []*gitalypb.FindAllBranchesResponse_Branch
	for _, name := range expectedRefs {
		target, ok := localBranches[name]
		require.True(t, ok)

		branch := &gitalypb.FindAllBranchesResponse_Branch{
			Name:   []byte(name),
			Target: target,
		}
		expectedBranches = append(expectedBranches, branch)
	}

	masterCommit, err := repo.ReadCommit(ctx, "master")
	require.NoError(t, err)
	expectedBranches = append(expectedBranches, &gitalypb.FindAllBranchesResponse_Branch{
		Name:   []byte("refs/heads/master"),
		Target: masterCommit,
	})

	testCases := []struct {
		desc             string
		request          *gitalypb.FindAllBranchesRequest
		expectedBranches []*gitalypb.FindAllBranchesResponse_Branch
	}{
		{
			desc: "all merged branches",
			request: &gitalypb.FindAllBranchesRequest{
				Repository: repoProto,
				MergedOnly: true,
			},
			expectedBranches: expectedBranches,
		},
		{
			desc: "all merged from a list of branches",
			request: &gitalypb.FindAllBranchesRequest{
				Repository: repoProto,
				MergedOnly: true,
				MergedBranches: [][]byte{
					[]byte("refs/heads/100%branch"),
					[]byte("refs/heads/improve/awesome"),
					[]byte("refs/heads/gitaly-stuff"),
				},
			},
			expectedBranches: expectedBranches[:2],
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			c, err := client.FindAllBranches(ctx, testCase.request)
			require.NoError(t, err)

			branches := readFindAllBranchesResponsesFromClient(t, c)
			require.Len(t, branches, len(testCase.expectedBranches))

			for _, branch := range branches {
				// The GitCommit object returned by GetCommit() above and the one returned in the response
				// vary a lot. We can't guarantee that master will be fixed at a certain commit so we can't create
				// a structure for it manually, hence this hack.
				if string(branch.Name) == "refs/heads/master" {
					continue
				}

				assertContainsAllBranchesResponseBranch(t, testCase.expectedBranches, branch)
			}
		})
	}
}

func TestInvalidFindAllBranchesRequest(t *testing.T) {
	t.Parallel()

	_, client := setupRefServiceWithoutRepo(t)

	for _, tc := range []struct {
		description string
		request     *gitalypb.FindAllBranchesRequest
		expectedErr error
	}{
		{
			description: "Empty request",
			request:     &gitalypb.FindAllBranchesRequest{},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			description: "Invalid repo",
			request: &gitalypb.FindAllBranchesRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "repo",
				},
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"creating object reader: GetStorageByName: no such storage: \"fake\"",
				"repo scoped: invalid Repository",
			)),
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			ctx := testhelper.Context(t)
			c, err := client.FindAllBranches(ctx, tc.request)
			require.NoError(t, err)

			var recvError error
			for recvError == nil {
				_, recvError = c.Recv()
			}

			testhelper.RequireGrpcError(t, tc.expectedErr, recvError)
		})
	}
}

func readFindAllBranchesResponsesFromClient(t *testing.T, c gitalypb.RefService_FindAllBranchesClient) (branches []*gitalypb.FindAllBranchesResponse_Branch) {
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		branches = append(branches, r.GetBranches()...)
	}

	return
}
