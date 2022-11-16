//go:build !gitaly_test_sha256

package operations

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServer_UserCherryPick_successful(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	masterHeadCommit, err := repo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	testRepoCopy, testRepoCopyPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	}) // read-only repo

	testCases := []struct {
		desc         string
		getRequest   func() *gitalypb.UserCherryPickRequest
		branchUpdate *gitalypb.OperationBranchUpdate
	}{
		{
			desc: "branch exists",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				destinationBranch := "cherry-picking-dst"
				gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

				gittest.Exec(t, cfg, "-C", testRepoCopyPath, "branch", destinationBranch, "master")

				return &gitalypb.UserCherryPickRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					Commit:     cherryPickedCommit,
					BranchName: []byte(destinationBranch),
					Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "branch exists + existingOldOID",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				destinationBranch := "cherry-picking-dst-old-OID"
				gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

				gittest.Exec(t, cfg, "-C", testRepoCopyPath, "branch", destinationBranch, "master")

				return &gitalypb.UserCherryPickRequest{
					Repository:     repoProto,
					User:           gittest.TestUser,
					Commit:         cherryPickedCommit,
					ExpectedOldOid: masterHeadCommit.GetId(),
					BranchName:     []byte(destinationBranch),
					Message:        []byte("Cherry-picking " + cherryPickedCommit.Id),
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      repoProto,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-1"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartBranchName: []byte("master"),
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      repoProto,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-2"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartRepository: testRepoCopy,
					StartBranchName: []byte("master"),
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      repoProto,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-3"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartBranchName: []byte("master"),
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "branch exists with dry run",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				destinationBranch := "cherry-picking-dst-dry-run"
				gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

				gittest.Exec(t, cfg, "-C", testRepoCopyPath, "branch", destinationBranch, "master")
				return &gitalypb.UserCherryPickRequest{
					Repository: testRepoCopy,
					User:       gittest.TestUser,
					Commit:     cherryPickedCommit,
					BranchName: []byte(destinationBranch),
					Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
					DryRun:     true,
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository with dry run",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      testRepoCopy,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-1"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartBranchName: []byte("master"),
					DryRun:          true,
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository with dry run",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      testRepoCopy,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-2"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartRepository: testRepoCopy,
					StartBranchName: []byte("master"),
					DryRun:          true,
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository with dry run",
			getRequest: func() *gitalypb.UserCherryPickRequest {
				return &gitalypb.UserCherryPickRequest{
					Repository:      testRepoCopy,
					User:            gittest.TestUser,
					Commit:          cherryPickedCommit,
					BranchName:      []byte("to-be-cherry-picked-into-3"),
					Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
					StartBranchName: []byte("master"),
					DryRun:          true,
				}
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := testCase.getRequest()

			response, err := client.UserCherryPick(ctx, request)
			require.NoError(t, err)

			testRepo := localrepo.NewTestRepo(t, cfg, request.Repository)
			headCommit, err := testRepo.ReadCommit(ctx, git.Revision(request.BranchName))
			require.NoError(t, err)

			expectedBranchUpdate := testCase.branchUpdate
			expectedBranchUpdate.CommitId = headCommit.Id

			require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
			//nolint:staticcheck
			require.Empty(t, response.CreateTreeError)
			//nolint:staticcheck
			require.Empty(t, response.CreateTreeErrorCode)

			if request.DryRun {
				testhelper.ProtoEqual(t, masterHeadCommit, headCommit)
			} else {
				require.Equal(t, request.Message, headCommit.Subject)
				require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
			}
		})
	}
}

func TestServer_UserCherryPick_successfulGitHooks(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	var hookOutputFiles []string
	for _, hookName := range GitlabHooks {
		hookOutputTempPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)
		hookOutputFiles = append(hookOutputFiles, hookOutputTempPath)
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	//nolint:staticcheck
	require.Empty(t, response.PreReceiveError)

	for _, file := range hookOutputFiles {
		output := string(testhelper.MustReadFile(t, file))
		require.Contains(t, output, "GL_USERNAME="+gittest.TestUser.GlUsername)
	}
}

func TestServer_UserCherryPick_stableID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	commitToPick, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     commitToPick,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + commitToPick.Id),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12345},
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	//nolint:staticcheck
	require.Empty(t, response.PreReceiveError)
	require.Equal(t, "b17aeac93194cf2385b32623494ebce66efbacad", response.BranchUpdate.CommitId)

	pickedCommit, err := repo.ReadCommit(ctx, git.Revision(response.BranchUpdate.CommitId))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:        "b17aeac93194cf2385b32623494ebce66efbacad",
		Subject:   []byte("Cherry-picking " + commitToPick.Id),
		Body:      []byte("Cherry-picking " + commitToPick.Id),
		BodySize:  55,
		ParentIds: []string{"1e292f8fedd741b75372e19097c76d327140c312"},
		TreeId:    "5f1b6bcadf0abc482a19454aeaa219a5998db083",
		Author: &gitalypb.CommitAuthor{
			Name:  []byte("Ahmad Sherif"),
			Email: []byte("me@ahmadsherif.com"),
			Date: &timestamppb.Timestamp{
				Seconds: 1487337076,
			},
			Timezone: []byte("+0200"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			Date: &timestamppb.Timestamp{
				Seconds: 12345,
			},
			Timezone: []byte("+0000"),
		},
	}, pickedCommit)
}

func TestServer_UserCherryPick_failedValidations(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	testCases := []struct {
		desc        string
		request     *gitalypb.UserCherryPickRequest
		expectedErr error
	}{
		{
			desc: "no repository provided",
			request: &gitalypb.UserCherryPickRequest{
				Repository: nil,
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty user",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       nil,
				Commit:     cherryPickedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			expectedErr: status.Error(codes.InvalidArgument, "empty User"),
		},
		{
			desc: "empty commit",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     nil,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			expectedErr: status.Error(codes.InvalidArgument, "empty Commit"),
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     cherryPickedCommit,
				BranchName: nil,
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			expectedErr: status.Error(codes.InvalidArgument, "empty BranchName"),
		},
		{
			desc: "empty message",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     cherryPickedCommit,
				BranchName: []byte(destinationBranch),
				Message:    nil,
			},
			expectedErr: status.Error(codes.InvalidArgument, "empty Message"),
		},
		{
			desc: "commit not found",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     &gitalypb.GitCommit{Id: "will-not-be-found"},
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking not found"),
			},
			expectedErr: status.Error(codes.NotFound, `cherry-pick: commit lookup: commit not found: "will-not-be-found"`),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			_, err := client.UserCherryPick(ctx, testCase.request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func TestServer_UserCherryPick_failedWithPreReceiveError(t *testing.T) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, testhelper.Context(t))

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			response, err := client.UserCherryPick(ctx, request)
			require.Nil(t, response)
			testhelper.RequireGrpcError(t, errWithDetails(t,
				helper.ErrFailedPrecondition(
					errors.New("access check failed"),
				),
				&gitalypb.UserCherryPickError{
					Error: &gitalypb.UserCherryPickError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "GL_ID=user-123",
						},
					},
				},
			), err)
		})
	}
}

func TestUser_UserCherryPick_expectedOldOID(t *testing.T) {
	t.Parallel()

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, testhelper.Context(t))

	testCases := []struct {
		desc        string
		setup       func(*gitalypb.Repository, string) *gitalypb.UserCherryPickRequest
		expectedErr error
	}{
		{
			desc: "invalid ExpectedOldOID",
			setup: func(repoProto *gitalypb.Repository, repoPath string) *gitalypb.UserCherryPickRequest {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
				))

				destinationBranch := "cherry-picking-dst"
				gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
				))
				cherryPickedCommit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				return &gitalypb.UserCherryPickRequest{
					Repository:     repoProto,
					User:           gittest.TestUser,
					Commit:         cherryPickedCommit,
					BranchName:     []byte(destinationBranch),
					ExpectedOldOid: "foobar",
					Message:        []byte("Cherry-picking " + cherryPickedCommit.Id),
				}
			},
			expectedErr: status.Error(codes.InvalidArgument, "resolve ref: reference not found"),
		},
		{
			desc: "valid but incorrect ExpectedOldOID",
			setup: func(repoProto *gitalypb.Repository, repoPath string) *gitalypb.UserCherryPickRequest {
				commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
				))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(commit1), gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
				))

				destinationBranch := "cherry-picking-dst"
				gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

				headMinusOne := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", destinationBranch+"~1"))

				repo := localrepo.NewTestRepo(t, cfg, repoProto)
				commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
				))
				cherryPickedCommit, err := repo.ReadCommit(ctx, commitID.Revision())
				require.NoError(t, err)

				return &gitalypb.UserCherryPickRequest{
					Repository:     repoProto,
					User:           gittest.TestUser,
					Commit:         cherryPickedCommit,
					BranchName:     []byte(destinationBranch),
					ExpectedOldOid: headMinusOne,
					Message:        []byte("Cherry-picking " + cherryPickedCommit.Id),
				}
			},
			expectedErr: status.Error(codes.Internal, "update reference with hooks: Could not update refs/heads/cherry-picking-dst. Please refresh and try again."),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			request := tc.setup(repoProto, repoPath)

			_, err := client.UserCherryPick(ctx, request)
			require.Equal(t, err, tc.expectedErr)
		})
	}
}

func TestServer_UserCherryPick_failedWithCreateTreeError(t *testing.T) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, testhelper.Context(t))

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	// This commit already exists in master
	cherryPickedCommit, err := repo.ReadCommit(ctx, "4a24d82dbca5c11c61556f3b35ca472b7463187e")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.Nil(t, response)
	testhelper.RequireGrpcError(t, errWithDetails(t,
		helper.ErrFailedPrecondition(
			errors.New("cherry-pick: could not apply because the result was empty"),
		),
		&gitalypb.UserCherryPickError{
			Error: &gitalypb.UserCherryPickError_ChangesAlreadyApplied{},
		},
	), err)
}

func TestServer_UserCherryPick_failedWithCommitError(t *testing.T) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, testhelper.Context(t))

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	sourceBranch := "cherry-pick-src"
	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")
	gittest.Exec(t, cfg, "-C", repoPath, "branch", sourceBranch, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")

	cherryPickedCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository:      repoProto,
		User:            gittest.TestUser,
		Commit:          cherryPickedCommit,
		BranchName:      []byte(sourceBranch),
		Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
		StartBranchName: []byte(destinationBranch),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.Nil(t, response)
	s, ok := status.FromError(err)
	require.True(t, ok)

	details := s.Details()
	require.Len(t, details, 1)
	detailedErr, ok := details[0].(*gitalypb.UserCherryPickError)
	require.True(t, ok)

	targetBranchDivergedErr, ok := detailedErr.Error.(*gitalypb.UserCherryPickError_TargetBranchDiverged)
	require.True(t, ok)
	assert.Equal(t, []byte("8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"), targetBranchDivergedErr.TargetBranchDiverged.ParentRevision)
}

func TestServerUserCherryPickRailedWithConflict(t *testing.T) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, testhelper.Context(t))

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "cherry-picking-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "conflict_branch_a")

	// This commit cannot be applied to the destinationBranch above
	cherryPickedCommit, err := repo.ReadCommit(ctx, git.Revision("f0f390655872bb2772c85a0128b2fbc2d88670cb"))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.Nil(t, response)
	s, ok := status.FromError(err)
	require.True(t, ok)

	details := s.Details()
	require.Len(t, details, 1)
	detailedErr, ok := details[0].(*gitalypb.UserCherryPickError)
	require.True(t, ok)

	conflictErr, ok := detailedErr.Error.(*gitalypb.UserCherryPickError_CherryPickConflict)
	require.True(t, ok)
	require.Len(t, conflictErr.CherryPickConflict.ConflictingFiles, 1)
	assert.Equal(t, []byte("NEW_FILE.md"), conflictErr.CherryPickConflict.ConflictingFiles[0])
}

func TestServer_UserCherryPick_successfulWithGivenCommits(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testCases := []struct {
		desc           string
		startRevision  git.Revision
		cherryRevision git.Revision
	}{
		{
			desc:           "merge commit",
			startRevision:  "281d3a76f31c812dbf48abce82ccf6860adedd81",
			cherryRevision: "6907208d755b60ebeacb2e9dfea74c92c3449a1f",
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			destinationBranch := fmt.Sprintf("cherry-picking-%d", i)

			gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, testCase.startRevision.String())

			commit, err := repo.ReadCommit(ctx, testCase.cherryRevision)
			require.NoError(t, err)

			request := &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     commit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + testCase.cherryRevision.String()),
			}

			response, err := client.UserCherryPick(ctx, request)
			require.NoError(t, err)

			newHead, err := repo.ReadCommit(ctx, git.Revision(destinationBranch))
			require.NoError(t, err)

			expectedResponse := &gitalypb.UserCherryPickResponse{
				BranchUpdate: &gitalypb.OperationBranchUpdate{CommitId: newHead.Id},
			}

			testhelper.ProtoEqual(t, expectedResponse, response)

			require.Equal(t, request.Message, newHead.Subject)
			require.Equal(t, testCase.startRevision.String(), newHead.ParentIds[0])
		})
	}
}

func TestServer_UserCherryPick_quarantine(t *testing.T) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, testhelper.Context(t))
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Set up a hook that parses the new object and then aborts the update. Like this, we can
	// assert that the object does not end up in the main repository.
	outputPath := filepath.Join(testhelper.TempDir(t), "output")
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(
		`#!/bin/sh
		read oldval newval ref &&
		git rev-parse $newval^{commit} >%s &&
		exit 1
	`, outputPath)))

	commit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     commit,
		BranchName: []byte("refs/heads/master"),
		Message:    []byte("Message"),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.Nil(t, response)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "access check failed")

	hookOutput := testhelper.MustReadFile(t, outputPath)
	oid, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(hookOutput))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	require.False(t, exists, "quarantined commit should have been discarded")
}
