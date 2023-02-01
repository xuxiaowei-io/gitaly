//go:build !gitaly_test_sha256

package operations

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUserRevert(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	branchName := "revert-branch"

	type setupData struct {
		expectedCommitID string
		repoPath         string
		request          *gitalypb.UserRevertRequest
	}

	testCases := []struct {
		desc             string
		setup            func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData
		expectedResponse *gitalypb.UserRevertResponse
		expectedErr      error
	}{
		{
			desc: "successful",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						Commit:     firstCommit,
						BranchName: []byte(branchName),
						Message:    []byte("Reverting " + firstCommitID),
					},
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
			expectedErr:      nil,
		},
		{
			desc: "nonexistent branch + start_repository == repository",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository:      repoProto,
						User:            gittest.TestUser,
						Commit:          firstCommit,
						BranchName:      []byte(branchName),
						StartBranchName: []byte("master"),
						Message:         []byte("Reverting " + firstCommitID),
					},
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{
				BranchCreated: true,
			}},
			expectedErr: nil,
		},
		{
			desc: "nonexistent branch + start_repository != repository",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				startRepoProto, startRepoPath := gittest.CreateRepository(t, ctx, cfg)
				startRepo := localrepo.NewTestRepo(t, cfg, startRepoProto)

				firstCommitID := gittest.WriteCommit(t, cfg, startRepoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := startRepo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository:      repoProto,
						User:            gittest.TestUser,
						Commit:          firstCommit,
						BranchName:      []byte(branchName),
						StartBranchName: []byte("master"),
						StartRepository: startRepoProto,
						Message:         []byte("Reverting " + firstCommitID),
					},
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true, RepoCreated: true}},
			expectedErr:      nil,
		},
		{
			desc: "successful with dry run",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						Commit:     firstCommit,
						BranchName: []byte(branchName),
						Message:    []byte("Reverting " + firstCommitID),
						DryRun:     true,
					},
					expectedCommitID: firstCommit.Id,
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
			expectedErr:      nil,
		},
		{
			desc: "nonexistent branch + start_repository == repository with dry run",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository:      repoProto,
						User:            gittest.TestUser,
						Commit:          firstCommit,
						BranchName:      []byte(branchName),
						StartBranchName: []byte("master"),
						Message:         []byte("Reverting " + firstCommitID),
						DryRun:          true,
					},
					expectedCommitID: firstCommit.Id,
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{
				BranchCreated: true,
			}},
			expectedErr: nil,
		},
		{
			desc: "nonexistent branch + start_repository != repository with dry run",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				startRepoProto, startRepoPath := gittest.CreateRepository(t, ctx, cfg)
				startRepo := localrepo.NewTestRepo(t, cfg, startRepoProto)

				firstCommitID := gittest.WriteCommit(t, cfg, startRepoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := startRepo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository:      repoProto,
						User:            gittest.TestUser,
						Commit:          firstCommit,
						BranchName:      []byte(branchName),
						StartBranchName: []byte("master"),
						StartRepository: startRepoProto,
						Message:         []byte("Reverting " + firstCommitID),
						DryRun:          true,
					},
					expectedCommitID: firstCommitID.String(),
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true, RepoCreated: true}},
			expectedErr:      nil,
		},

		{
			desc: "no repository provided",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository: nil,
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty user",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty User"),
		},
		{
			desc: "empty commit",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty Commit"),
		},
		{
			desc: "empty branch name",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						Commit:     firstCommit,
						Message:    []byte("Reverting " + firstCommitID),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty BranchName"),
		},
		{
			desc: "empty message",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository: repoProto,
						User:       gittest.TestUser,
						Commit:     firstCommit,
						BranchName: []byte(branchName),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty Message"),
		},
		{
			desc: "successful + expectedOldOID",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					repoPath: repoPath,
					request: &gitalypb.UserRevertRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						Commit:         firstCommit,
						BranchName:     []byte(branchName),
						Message:        []byte("Reverting " + firstCommitID),
						ExpectedOldOid: firstCommitID.String(),
					},
				}
			},
			expectedResponse: &gitalypb.UserRevertResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{}},
			expectedErr:      nil,
		},
		{
			desc: "successful + invalid expectedOldOID",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						Commit:         firstCommit,
						BranchName:     []byte(branchName),
						Message:        []byte("Reverting " + firstCommitID),
						ExpectedOldOid: "foobar",
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument(fmt.Sprintf(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())).
				WithInterceptedMetadata("old_object_id", "foobar"),
		},
		{
			desc: "expectedOldOID with valid SHA, but not present in repo",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "foobar"},
				))
				firstCommit, err := repo.ReadCommit(ctx, firstCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						Commit:         firstCommit,
						BranchName:     []byte(branchName),
						Message:        []byte("Reverting " + firstCommitID),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
				}
			},
			expectedErr: structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found").
				WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID),
		},
		{
			desc: "expectedOldOID pointing to old commit",
			setup: func(t *testing.T, repoPath string, repoProto *gitalypb.Repository, repo *localrepo.Repo) setupData {
				firstCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "blob", Mode: "100644", Content: "bar"},
				))
				secondCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(firstCommitID), gittest.WithBranch(branchName), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "bolb", Mode: "100644", Content: "foo"},
				))
				secondCommit, err := repo.ReadCommit(ctx, secondCommitID.Revision())
				require.NoError(t, err)

				return setupData{
					request: &gitalypb.UserRevertRequest{
						Repository:     repoProto,
						User:           gittest.TestUser,
						Commit:         secondCommit,
						BranchName:     []byte(branchName),
						Message:        []byte("Reverting " + secondCommitID),
						ExpectedOldOid: firstCommitID.String(),
					},
				}
			},
			expectedErr: structerr.NewInternal("update reference with hooks: Could not update refs/heads/%s. Please refresh and try again.", branchName),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			data := tc.setup(t, repoPath, repoProto, repo)

			response, err := client.UserRevert(ctx, data.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			if tc.expectedErr != nil {
				return
			}

			branchCommitID := text.ChompBytes(gittest.Exec(t, cfg, "-C", data.repoPath, "rev-parse", branchName))
			tc.expectedResponse.BranchUpdate.CommitId = branchCommitID

			// For dry-run, we only skip the `update-ref` section, so a non-existent branch
			// will be created by `UserRevert`. But, we need to ensure that the
			// expectedCommitID of the branch on which we requested revert doesn't change.
			if data.expectedCommitID != "" {
				require.Equal(t, data.expectedCommitID, branchCommitID, "dry run should point at expected commit")
			}

			testhelper.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestServer_UserRevert_quarantine(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
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

	commitToRevert, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	response, err := client.UserRevert(ctx, &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     commitToRevert,
		BranchName: []byte("master"),
		Message:    []byte("Reverting commit"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12345},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NotEmpty(t, response.PreReceiveError)

	hookOutput := testhelper.MustReadFile(t, outputPath)
	oid, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(hookOutput))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	require.False(t, exists, "quarantined commit should have been discarded")
}

func TestServer_UserRevert_stableID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, _, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	commitToRevert, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	response, err := client.UserRevert(ctx, &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     commitToRevert,
		BranchName: []byte("master"),
		Message:    []byte("Reverting commit"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12345},
	})
	require.NoError(t, err)

	require.Equal(t, &gitalypb.OperationBranchUpdate{
		CommitId: "9c15289b0a129c562dddf7b364eb979d41173b41",
	}, response.BranchUpdate)
	require.Empty(t, response.CreateTreeError)
	require.Empty(t, response.CreateTreeErrorCode)

	revertedCommit, err := repo.ReadCommit(ctx, git.Revision("master"))
	require.NoError(t, err)

	require.Equal(t, &gitalypb.GitCommit{
		Id: "9c15289b0a129c562dddf7b364eb979d41173b41",
		ParentIds: []string{
			"1e292f8fedd741b75372e19097c76d327140c312",
		},
		TreeId:   "3a1de94946517a42fcfe4bf4986b8c61af799bd5",
		Subject:  []byte("Reverting commit"),
		Body:     []byte("Reverting commit"),
		BodySize: 16,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Jane Doe"),
			Email:    []byte("janedoe@gitlab.com"),
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     []byte("Jane Doe"),
			Email:    []byte("janedoe@gitlab.com"),
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	}, revertedCommit)
}

func TestServer_UserRevert_successfulIntoEmptyRepo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, startRepoProto, _, client := setupOperationsService(t, ctx)

	startRepo := localrepo.NewTestRepo(t, cfg, startRepoProto)

	revertedCommit, err := startRepo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	masterHeadCommit, err := startRepo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	request := &gitalypb.UserRevertRequest{
		Repository:      repoProto,
		User:            gittest.TestUser,
		Commit:          revertedCommit,
		BranchName:      []byte("dst-branch"),
		Message:         []byte("Reverting " + revertedCommit.Id),
		StartRepository: startRepoProto,
		StartBranchName: []byte("master"),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)

	headCommit, err := repo.ReadCommit(ctx, git.Revision(request.BranchName))
	require.NoError(t, err)

	expectedBranchUpdate := &gitalypb.OperationBranchUpdate{
		BranchCreated: true,
		RepoCreated:   true,
		CommitId:      headCommit.Id,
	}

	require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
	require.Empty(t, response.CreateTreeError)
	require.Empty(t, response.CreateTreeErrorCode)
	require.Equal(t, request.Message, headCommit.Subject)
	require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
}

func TestServer_UserRevert_successfulGitHooks(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	var hookOutputFiles []string
	for _, hookName := range GitlabHooks {
		hookOutputTempPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)
		hookOutputFiles = append(hookOutputFiles, hookOutputTempPath)
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)

	for _, file := range hookOutputFiles {
		output := string(testhelper.MustReadFile(t, file))
		require.Contains(t, output, "GL_USERNAME="+gittest.TestUser.GlUsername)
	}
}

func TestServer_UserRevert_failedDueToPreReceiveError(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			response, err := client.UserRevert(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+gittest.TestUser.GlId)
		})
	}
}

func TestServer_UserRevert_failedDueToCreateTreeErrorConflict(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	// This revert patch of the following commit cannot be applied to the destinationBranch above
	revertedCommit, err := repo.ReadCommit(ctx, "372ab6950519549b14d220271ee2322caa44d4eb")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_CONFLICT, response.CreateTreeErrorCode)
}

func TestServer_UserRevert_failedDueToCreateTreeErrorEmpty(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_NONE, response.CreateTreeErrorCode)

	response, err = client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_EMPTY, response.CreateTreeErrorCode)
}

func TestServer_UserRevert_failedDueToCommitError(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	sourceBranch := "revert-src"
	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")
	gittest.Exec(t, cfg, "-C", repoPath, "branch", sourceBranch, "a5391128b0ef5d21df5dd23d98557f4ef12fae20")

	revertedCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository:      repoProto,
		User:            gittest.TestUser,
		Commit:          revertedCommit,
		BranchName:      []byte(destinationBranch),
		Message:         []byte("Reverting " + revertedCommit.Id),
		StartBranchName: []byte(sourceBranch),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Branch diverged", response.CommitError)
}
