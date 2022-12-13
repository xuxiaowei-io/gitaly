//go:build !gitaly_test_sha256

package diff

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestGetPatchID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, repoProto, _, client := setupDiffService(t, ctx)

	testCases := []struct {
		desc             string
		setup            func() *gitalypb.GetPatchIDRequest
		expectedResponse *gitalypb.GetPatchIDResponse
		expectedErr      error
	}{
		{
			desc: "returns patch-id successfully",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("HEAD~3"),
					NewRevision: []byte("HEAD"),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{PatchId: "1d108b99acd03f94e015c5237afa7bc24382f1f6"},
		},
		{
			desc: "returns patch-id successfully with commit ids",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("55bc176024cfa3baaceb71db584c7e5df900ea65"),
					NewRevision: []byte("c1c67abbaf91f624347bb3ae96eabe3a1b742478"),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{PatchId: "1c5c57c2b6ef85b3cd248c34318bcce5bec3b540"},
		},
		{
			desc: "returns patch-id successfully for a specific file",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("HEAD~3:.gitattributes"),
					NewRevision: []byte("HEAD:.gitattributes"),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{PatchId: "ed52511a91dd53dcf58acf6e85be5456b54f2f8a"},
		},
		{
			desc: "file didn't exist in the old revision",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("HEAD~3:files/flat/path/correct/content.txt"),
					NewRevision: []byte("HEAD:files/flat/path/correct/content.txt"),
				}
			},
			expectedErr: structerr.New("waiting for git-diff: exit status 128"),
		},
		{
			desc: "unknown revisions",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("0000000000000000000000000000000000000000"),
					NewRevision: []byte("1111111111111111111111111111111111111111"),
				}
			},
			expectedErr: structerr.New("waiting for git-diff: exit status 128").WithMetadata("stderr", ""),
		},
		{
			desc: "no diff from the given revisions",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("HEAD"),
					NewRevision: []byte("HEAD"),
				}
			},
			expectedErr: structerr.NewFailedPrecondition("no difference between old and new revision"),
		},
		{
			desc: "empty repository",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  nil,
					OldRevision: []byte("HEAD~1"),
					NewRevision: []byte("HEAD"),
				}
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty old revision",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					NewRevision: []byte("HEAD"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty OldRevision"),
		},
		{
			desc: "empty new revision",
			setup: func() *gitalypb.GetPatchIDRequest {
				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("HEAD~1"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty NewRevision"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			request := tc.setup()
			response, err := client.GetPatchID(ctx, request)

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
