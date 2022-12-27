//go:build !gitaly_test_sha256

package diff

import (
	"fmt"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestGetPatchID(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffServiceWithoutRepo(t)

	testCases := []struct {
		desc             string
		setup            func(t *testing.T) *gitalypb.GetPatchIDRequest
		expectedResponse *gitalypb.GetPatchIDResponse
		expectedErr      error
	}{
		{
			desc: "retruns patch-id successfully",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "old"},
					),
				)
				gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithBranch("main"),
					gittest.WithParents(oldCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "new"},
					),
				)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte("main~"),
					NewRevision: []byte("main"),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{
				PatchId: "a79c7e9df0094ee44fa7a2a9ae27e914e6b7e00b",
			},
		},
		{
			desc: "returns patch-id successfully with commit ids",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "old"},
					),
				)
				newCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "new"},
					),
				)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte(oldCommit),
					NewRevision: []byte(newCommit),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{
				PatchId: "a79c7e9df0094ee44fa7a2a9ae27e914e6b7e00b",
			},
		},
		{
			desc: "returns patch-id successfully for a specific file",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "old"},
					),
				)
				newCommit := gittest.WriteCommit(t, cfg, repoPath,
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "file", Mode: "100644", Content: "new"},
					),
				)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte(fmt.Sprintf("%s:file", oldCommit)),
					NewRevision: []byte(fmt.Sprintf("%s:file", newCommit)),
				}
			},
			expectedResponse: &gitalypb.GetPatchIDResponse{
				PatchId: "a79c7e9df0094ee44fa7a2a9ae27e914e6b7e00b",
			},
		},
		{
			desc: "file didn't exist in the old revision",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				oldCommit := gittest.WriteCommit(t, cfg, repoPath)
				newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file", Mode: "100644", Content: "new"},
				))

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte(fmt.Sprintf("%s:file", oldCommit)),
					NewRevision: []byte(fmt.Sprintf("%s:file", newCommit)),
				}
			},
			expectedErr: structerr.New("waiting for git-diff: exit status 128"),
		},
		{
			desc: "unknown revisions",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

				newRevision := strings.Replace(string(gittest.DefaultObjectHash.ZeroOID), "0", "1", -1)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte(gittest.DefaultObjectHash.ZeroOID),
					NewRevision: []byte(newRevision),
				}
			},
			expectedErr: structerr.New("waiting for git-diff: exit status 128").WithMetadata("stderr", ""),
		},
		{
			desc: "no diff from the given revisions",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commit := gittest.WriteCommit(t, cfg, repoPath)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					OldRevision: []byte(commit),
					NewRevision: []byte(commit),
				}
			},
			expectedErr: structerr.NewFailedPrecondition("no difference between old and new revision"),
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
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
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

				return &gitalypb.GetPatchIDRequest{
					Repository:  repoProto,
					NewRevision: []byte("HEAD"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty OldRevision"),
		},
		{
			desc: "empty new revision",
			setup: func(t *testing.T) *gitalypb.GetPatchIDRequest {
				repoProto, _ := gittest.CreateRepository(t, ctx, cfg)

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

			request := tc.setup(t)
			response, err := client.GetPatchID(ctx, request)

			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}
