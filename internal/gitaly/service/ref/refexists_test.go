//go:build !gitaly_test_sha256

package ref

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRefExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRefService(t, ctx)

	for _, tc := range []struct {
		desc             string
		request          *gitalypb.RefExistsRequest
		expectedResponse *gitalypb.RefExistsResponse
		expectedErr      error
	}{
		{
			desc: "master",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/master"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "v1.0.0",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/tags/v1.0.0"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "quoted",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/'test'"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "unicode exists",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/ʕ•ᴥ•ʔ"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: true,
			},
		},
		{
			desc: "unicode missing",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/tags/अस्तित्वहीन"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "spaces",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/ /heads"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "haxxors",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/; rm -rf /tmp/*"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "dashes",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("--"),
			},
			expectedErr: structerr.NewInvalidArgument("invalid refname"),
		},
		{
			desc: "blank",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte(""),
			},
			expectedErr: structerr.NewInvalidArgument("invalid refname"),
		},
		{
			desc: "not tags or branches",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/remotes/origin"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "wildcards",
			request: &gitalypb.RefExistsRequest{
				Repository: repo,
				Ref:        []byte("refs/heads/*"),
			},
			expectedResponse: &gitalypb.RefExistsResponse{
				Value: false,
			},
		},
		{
			desc: "invalid repos",
			request: &gitalypb.RefExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "invalid",
					RelativePath: "/etc/",
				},
				Ref: []byte("refs/heads/master"),
			},
			expectedErr: testhelper.GitalyOrPraefect(
				structerr.NewInvalidArgument("GetStorageByName: no such storage: %q", "invalid"),
				structerr.NewInvalidArgument("repo scoped: invalid Repository"),
			),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			response, err := client.RefExists(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestRefExists_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	_, repo, _, client := setupRefService(t, ctx)
	for _, tc := range []struct {
		desc        string
		req         *gitalypb.RefExistsRequest
		expectedErr error
	}{
		{
			desc: "repository not provided",
			req:  &gitalypb.RefExistsRequest{Repository: nil},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "invalid ref name",
			req:         &gitalypb.RefExistsRequest{Repository: repo, Ref: []byte("in valid")},
			expectedErr: status.Error(codes.InvalidArgument, "invalid refname"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RefExists(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}
