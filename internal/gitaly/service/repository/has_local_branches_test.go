//go:build !gitaly_test_sha256

package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulHasLocalBranches(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, client := setupRepositoryService(t, ctx)

	emptyRepo, _ := gittest.CreateRepository(t, ctx, cfg)

	testCases := []struct {
		desc      string
		request   *gitalypb.HasLocalBranchesRequest
		value     bool
		errorCode codes.Code
	}{
		{
			desc:    "repository has branches",
			request: &gitalypb.HasLocalBranchesRequest{Repository: repo},
			value:   true,
		},
		{
			desc: "repository doesn't have branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: emptyRepo,
			},
			value: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.HasLocalBranches(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))
			if err != nil {
				return
			}

			require.Equal(t, tc.value, response.Value)
		})
	}
}

func TestFailedHasLocalBranches(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc        string
		repository  *gitalypb.Repository
		expectedErr error
	}{
		{
			desc:       "repository nil",
			repository: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:       "repository doesn't exist",
			repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				`GetStorageByName: no such storage: "fake"`,
				"repo scoped: invalid Repository",
			)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			request := &gitalypb.HasLocalBranchesRequest{Repository: tc.repository}
			_, err := client.HasLocalBranches(ctx, request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
