//go:build !gitaly_test_sha256

package operations

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestUserFFBranch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	type setupData struct {
		repoPath         string
		request          *gitalypb.UserFFBranchRequest
		expectedResponse *gitalypb.UserFFBranchResponse
	}

	testCases := []struct {
		desc        string
		setup       func(t *testing.T, ctx context.Context) setupData
		expectedErr error
	}{
		{
			desc: "successful",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						CommitId:   commitToMerge.String(),
						Branch:     []byte("master"),
					},
					expectedResponse: &gitalypb.UserFFBranchResponse{
						BranchUpdate: &gitalypb.OperationBranchUpdate{
							CommitId: commitToMerge.String(),
						},
					},
				}
			},
			expectedErr: nil,
		},
		{
			desc: "successful + expectedOldOID",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						CommitId:       commitToMerge.String(),
						Branch:         []byte("master"),
						ExpectedOldOid: string(firstCommit),
					},
					expectedResponse: &gitalypb.UserFFBranchResponse{
						BranchUpdate: &gitalypb.OperationBranchUpdate{
							CommitId: commitToMerge.String(),
						},
					},
				}
			},
			expectedErr: nil,
		},
		{
			desc: "empty repository",
			setup: func(t *testing.T, ctx context.Context) setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						User:     gittest.TestUser,
						CommitId: commitToMerge.String(),
						Branch:   []byte("master"),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty user",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						CommitId:   commitToMerge.String(),
						Branch:     []byte("master"),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user"),
		},
		{
			desc: "empty commit",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						Branch:     []byte("master"),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty commit id"),
		},
		{
			desc: "non-existing commit",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						CommitId:   gittest.DefaultObjectHash.ZeroOID.String(),
						Branch:     []byte("master"),
					},
				}
			},
			expectedErr: structerr.NewInternal(`checking for ancestry: invalid commit: "%s"`, gittest.DefaultObjectHash.ZeroOID),
		},
		{
			desc: "empty branch",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						CommitId:   commitToMerge.String(),
						User:       gittest.TestUser,
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty branch name"),
		},
		{
			desc: "non-existing branch",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						CommitId:   commitToMerge.String(),
						User:       gittest.TestUser,
						Branch:     []byte("main"),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("reference not found"),
		},
		{
			desc: "commit is not a descendant of branch head",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file", Mode: "100644", Content: "something"},
				))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository: repoProto,
						CommitId:   commitToMerge.String(),
						User:       gittest.TestUser,
						Branch:     []byte("master"),
					},
				}
			},
			expectedErr: structerr.NewFailedPrecondition("not fast forward"),
		},
		{
			desc: "invalid expectedOldOID",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository:     repoProto,
						CommitId:       commitToMerge.String(),
						User:           gittest.TestUser,
						Branch:         []byte("master"),
						ExpectedOldOid: "foobar",
					},
				}
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument(fmt.Sprintf(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())),
				"old_object_id", "foobar"),
		},
		{
			desc: "valid SHA, but not existing expectedOldOID",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommit))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository:     repoProto,
						CommitId:       commitToMerge.String(),
						User:           gittest.TestUser,
						Branch:         []byte("master"),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
				}
			},
			expectedErr: testhelper.WithInterceptedMetadata(
				structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found"),
				"old_object_id", gittest.DefaultObjectHash.ZeroOID),
		},
		{
			desc: "expectedOldOID pointing to old commit",
			setup: func(t *testing.T, ctx context.Context) setupData {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

				firstCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "bar", Mode: "100644", Content: "something"},
				))
				secondCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(firstCommit),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Path: "foo", Mode: "100644", Content: "something"},
					),
				)
				commitToMerge := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(secondCommit), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "goo", Mode: "100644", Content: "something"},
				))

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserFFBranchRequest{
						Repository:     repoProto,
						CommitId:       commitToMerge.String(),
						User:           gittest.TestUser,
						Branch:         []byte("master"),
						ExpectedOldOid: firstCommit.String(),
					},
					// empty response is the expected (legacy) behavior when we fail to
					// update the ref.
					expectedResponse: &gitalypb.UserFFBranchResponse{},
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			data := tc.setup(t, ctx)

			resp, err := client.UserFFBranch(ctx, data.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, data.expectedResponse, resp)

			if data.expectedResponse != nil && data.expectedResponse.BranchUpdate != nil {
				newBranchHead := text.ChompBytes(gittest.Exec(t, cfg, "-C", data.repoPath, "rev-parse", string(data.request.Branch)))
				require.Equal(t, data.request.CommitId, newBranchHead, "branch head not updated")
			}
		})
	}
}

func TestUserFFBranch_failingHooks(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       gittest.TestUser,
	}

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			resp, err := client.UserFFBranch(ctx, request)
			require.Nil(t, err)
			require.Contains(t, resp.PreReceiveError, "failure")
		})
	}
}

func TestUserFFBranch_ambiguousReference(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	branchName := "test-ff-target-branch"

	// We're creating both a branch and a tag with the same name.
	// If `git rev-parse` is called on the branch name directly
	// without using the fully qualified reference, then it would
	// return the OID of the tag instead of the branch.
	//
	// In the past, this used to cause us to use the tag's OID as
	// old revision when calling git-update-ref. As a result, the
	// update would've failed as the branch's current revision
	// didn't match the specified old revision.
	gittest.Exec(t, cfg, "-C", repoPath,
		"branch", branchName,
		"6d394385cf567f80a8fd85055db1ab4c5295806f")
	gittest.Exec(t, cfg, "-C", repoPath, "tag", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f~")

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       gittest.TestUser,
	}
	expectedResponse := &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			RepoCreated:   false,
			BranchCreated: false,
			CommitId:      commitID,
		},
	}

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/"+branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}
