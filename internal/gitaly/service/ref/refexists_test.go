//go:build !gitaly_test_sha256

package ref

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRefExists(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRefService(t, ctx)

	badRepo := &gitalypb.Repository{StorageName: "invalid", RelativePath: "/etc/"}

	tests := []struct {
		name    string
		ref     string
		want    bool
		repo    *gitalypb.Repository
		wantErr codes.Code
	}{
		{"master", "refs/heads/master", true, repo, codes.OK},
		{"v1.0.0", "refs/tags/v1.0.0", true, repo, codes.OK},
		{"quoted", "refs/heads/'test'", true, repo, codes.OK},
		{"unicode exists", "refs/heads/ʕ•ᴥ•ʔ", true, repo, codes.OK},
		{"unicode missing", "refs/tags/अस्तित्वहीन", false, repo, codes.OK},
		{"spaces", "refs/ /heads", false, repo, codes.OK},
		{"haxxors", "refs/; rm -rf /tmp/*", false, repo, codes.OK},
		{"dashes", "--", false, repo, codes.InvalidArgument},
		{"blank", "", false, repo, codes.InvalidArgument},
		{"not tags or branches", "refs/heads/remotes/origin", false, repo, codes.OK},
		{"wildcards", "refs/heads/*", false, repo, codes.OK},
		{"invalid repos", "refs/heads/master", false, badRepo, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &gitalypb.RefExistsRequest{Repository: tt.repo, Ref: []byte(tt.ref)}

			got, err := client.RefExists(ctx, req)

			if helper.GrpcCode(err) != tt.wantErr {
				t.Errorf("server.RefExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr != codes.OK {
				if got != nil {
					t.Errorf("server.RefExists() = %v, want null", got)
				}
				return
			}

			if got.Value != tt.want {
				t.Errorf("server.RefExists() = %v, want %v", got.Value, tt.want)
			}
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
