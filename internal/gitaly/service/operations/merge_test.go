//go:build !gitaly_test_sha256

package operations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

func TestUserMergeBranch(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranch,
	)
}

func testUserMergeBranch(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	type setupData struct {
		commitToMerge string
		masterCommit  string
		branch        string
		message       string
		repoPath      string
		repoProto     *gitalypb.Repository
	}

	// note we don't compare the main response, but rather the OperationBranchUpdate
	// this is mostly because the main response contains the commit ID for the merged
	// commit which we can't generate beforehand.
	type setupResponse struct {
		firstRequest           *gitalypb.UserMergeBranchRequest
		firstExpectedResponse  *gitalypb.OperationBranchUpdate
		firstExpectedErr       error
		secondRequest          *gitalypb.UserMergeBranchRequest
		secondExpectedResponse *gitalypb.OperationBranchUpdate
		secondExpectedErr      func(response *gitalypb.UserMergeBranchResponse) error
	}

	testCases := []struct {
		desc  string
		hooks []string
		setup func(data setupData) setupResponse
	}{
		{
			desc:  "merge successful",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository: data.repoProto,
						User:       gittest.TestUser,
						CommitId:   data.commitToMerge,
						Branch:     []byte(data.branch),
						Message:    []byte(data.message),
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "merge + hooks",
			hooks: GitlabHooks,
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository: data.repoProto,
						User:       gittest.TestUser,
						CommitId:   data.commitToMerge,
						Branch:     []byte(data.branch),
						Message:    []byte(data.message),
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "merge successful + expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: data.masterCommit,
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
				}
			},
		},
		{
			desc:  "invalid expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: "foobar",
					},
					firstExpectedErr: structerr.NewInvalidArgument(fmt.Sprintf("invalid expected old object ID: invalid object ID: \"foobar\", expected length %v, got 6", gittest.DefaultObjectHash.EncodedLen())).
						WithInterceptedMetadata("old_object_id", "foobar"),
				}
			},
		},
		{
			desc:  "expectedOldOID not present in repo",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: gittest.DefaultObjectHash.ZeroOID.String(),
					},
					firstExpectedErr: structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found").
						WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID),
				}
			},
		},
		{
			desc:  "incorrect expectedOldOID",
			hooks: []string{},
			setup: func(data setupData) setupResponse {
				gittest.WriteCommit(t, cfg, data.repoPath,
					gittest.WithParents(git.ObjectID(data.masterCommit)),
					gittest.WithBranch(data.branch),
					gittest.WithTreeEntries(
						gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
						gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
					),
				)

				return setupResponse{
					firstRequest: &gitalypb.UserMergeBranchRequest{
						Repository:     data.repoProto,
						User:           gittest.TestUser,
						CommitId:       data.commitToMerge,
						Branch:         []byte(data.branch),
						Message:        []byte(data.message),
						ExpectedOldOid: data.masterCommit,
					},
					secondRequest:          &gitalypb.UserMergeBranchRequest{Apply: true},
					secondExpectedResponse: &gitalypb.OperationBranchUpdate{},
					secondExpectedErr: func(response *gitalypb.UserMergeBranchResponse) error {
						return structerr.NewFailedPrecondition("Could not update refs/heads/master. Please refresh and try again.").WithDetail(
							&gitalypb.UserMergeBranchError{
								Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
									ReferenceUpdate: &gitalypb.ReferenceUpdateError{
										ReferenceName: []byte("refs/heads/" + data.branch),
										OldOid:        data.masterCommit,
										NewOid:        response.GetCommitId(),
									},
								},
							},
						)
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			branchToMerge := "master"
			message := "Merged by Gitaly"

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

			masterCommitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branchToMerge),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
				),
			)
			mergeCommitID := gittest.WriteCommit(t, cfg, repoPath,
				gittest.WithParents(masterCommitID),
				gittest.WithTreeEntries(
					gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
					gittest.TreeEntry{Mode: "100644", Path: "foo", Content: "bar"},
				))

			data := tc.setup(setupData{
				commitToMerge: mergeCommitID.String(),
				masterCommit:  masterCommitID.String(),
				branch:        branchToMerge,
				message:       message,
				repoPath:      repoPath,
				repoProto:     repoProto,
			})

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			hookTempfiles := make([]string, len(tc.hooks))
			if len(tc.hooks) > 0 {
				tempDir := testhelper.TempDir(t)
				for i, hook := range tc.hooks {
					outputFile := filepath.Join(tempDir, hook)

					script := fmt.Sprintf("#!/bin/sh\n(cat && env) >%s \n", outputFile)
					gittest.WriteCustomHook(t, repoPath, hook, []byte(script))

					hookTempfiles[i] = outputFile
				}
			}

			require.NoError(t, mergeBidi.Send(data.firstRequest), "send first request")
			firstResponse, err := mergeBidi.Recv()

			if err != nil || data.firstExpectedErr != nil {
				testhelper.RequireGrpcError(t, data.firstExpectedErr, err)
				return
			}
			require.NoError(t, err, "receive first response")

			testhelper.ProtoEqual(t, data.firstExpectedResponse, firstResponse.BranchUpdate)

			if data.secondRequest == nil {
				return
			}

			if data.secondRequest.Apply && data.secondExpectedResponse != nil {
				data.secondExpectedResponse.CommitId = firstResponse.CommitId
			}

			require.NoError(t, mergeBidi.Send(data.secondRequest), "apply merge")
			secondResponse, err := mergeBidi.Recv()
			if data.secondExpectedErr != nil {
				if expectedErr := data.secondExpectedErr(firstResponse); err != nil || data.secondExpectedErr != nil {
					testhelper.RequireGrpcError(t, expectedErr, err)
					return
				}
			}
			require.NoError(t, err, "receive second response")

			testhelper.ProtoEqual(t, data.secondExpectedResponse, secondResponse.BranchUpdate)

			_, err = mergeBidi.Recv()
			require.Equal(t, io.EOF, err)

			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			commit, err := repo.ReadCommit(ctx, git.Revision(branchToMerge))
			require.NoError(t, err, "look up git commit after call has finished")

			require.Contains(t, commit.ParentIds, mergeCommitID.String())
			require.True(t, strings.HasPrefix(string(commit.Body), message), "expected %q to start with %q", commit.Body, message)

			if len(tc.hooks) > 0 {
				expectedGlID := "GL_ID=" + gittest.TestUser.GlId
				for i, h := range tc.hooks {
					hookEnv := testhelper.MustReadFile(t, hookTempfiles[i])

					lines := strings.Split(string(hookEnv), "\n")
					require.Contains(t, lines, expectedGlID, "expected env of hook %q to contain %q", h, expectedGlID)
					require.Contains(t, lines, "GL_PROTOCOL=web", "expected env of hook %q to contain GL_PROTOCOL")

					if h == "pre-receive" || h == "post-receive" {
						require.Regexp(t, masterCommitID.String()+" .* refs/heads/"+branchToMerge, lines[0], "expected env of hook %q to contain reference change", h)
					}
				}
			}
		})
	}
}

func TestUserMergeBranch_failure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	master := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "a", Content: "apple"},
	))
	commit1 := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
	))
	branchToMerge := "branchToMerge"
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(branchToMerge),
		gittest.WithParents(commit1),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "b", Content: "banana"},
		),
	)

	testCases := []struct {
		user             *gitalypb.User
		repo             *gitalypb.Repository
		desc             string
		commitID         string
		expectedOldOid   string
		branch           []byte
		message          []byte
		setup            func() *gitalypb.UserMergeBranchRequest
		expectedErr      error
		expectedApplyErr string
	}{
		{
			desc: "no repository provided",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					User: gittest.TestUser,
				}
			},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc: "empty user",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					CommitId:   master.Revision().String(),
					Branch:     []byte(branchToMerge),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user"),
		},
		{
			desc: "empty user name",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User: &gitalypb.User{
						GlId:       gittest.TestUser.GlId,
						GlUsername: gittest.TestUser.GlUsername,
						Email:      gittest.TestUser.Email,
						Timezone:   gittest.TestUser.Timezone,
					},
					CommitId: master.Revision().String(),
					Branch:   []byte(branchToMerge),
					Message:  []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user name"),
		},
		{
			desc: "empty user email",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User: &gitalypb.User{
						GlId:       gittest.TestUser.GlId,
						GlUsername: gittest.TestUser.GlUsername,
						Name:       gittest.TestUser.Name,
						Timezone:   gittest.TestUser.Timezone,
					},
					CommitId: master.Revision().String(),
					Branch:   []byte(branchToMerge),
					Message:  []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty user email"),
		},
		{
			desc: "empty commit",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					Branch:     []byte(branchToMerge),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty commit ID"),
		},
		{
			desc: "empty branch",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					CommitId:   master.Revision().String(),
					Message:    []byte("sample-message"),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty branch name"),
		},
		{
			desc: "empty message",
			setup: func() *gitalypb.UserMergeBranchRequest {
				return &gitalypb.UserMergeBranchRequest{
					Repository: repoProto,
					User:       gittest.TestUser,
					CommitId:   master.Revision().String(),
					Branch:     []byte(branchToMerge),
				}
			},
			expectedErr: structerr.NewInvalidArgument("empty message"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := tc.setup()
			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			require.NoError(t, mergeBidi.Send(request), "apply merge")
			_, err = mergeBidi.Recv()

			if tc.expectedErr != nil {
				testhelper.RequireGrpcError(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err, "receive first response")
			require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
			_, err = mergeBidi.Recv()

			require.EqualError(t, err, tc.expectedApplyErr)
		})
	}
}

func TestUserMergeBranch_quarantine(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchQuarantine,
	)
}

func testUserMergeBranchQuarantine(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	// Set up a hook that parses the merge commit and then aborts the update. Like this, we
	// can assert that the object does not end up in the main repository.
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(
		`#!/bin/sh
		read oldval newval ref &&
		git rev-parse $newval^{commit} >&2 &&
		git rev-parse $oldval^{commit} &&
		exit 1
	`))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	stream, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("Merged by Gitaly"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}))

	firstResponse, err := stream.Recv()
	require.NoError(t, err, "receive first response")

	require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	secondResponse, err := stream.Recv()
	testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("%s\n", firstResponse.CommitId).WithDetail(
		&gitalypb.UserMergeBranchError{
			Error: &gitalypb.UserMergeBranchError_CustomHook{
				CustomHook: &gitalypb.CustomHookError{
					HookType: gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
					Stdout:   []byte(fmt.Sprintf("%s\n", mergeBranchHeadBefore)),
					Stderr:   []byte(fmt.Sprintf("%s\n", firstResponse.CommitId)),
				},
			},
		},
	), err)
	require.Nil(t, secondResponse)

	oid, err := git.ObjectHashSHA1.FromHex(strings.TrimSpace(firstResponse.CommitId))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	require.False(t, exists, "quarantined commit should have been discarded")
}

func TestUserMergeBranch_stableMergeIDs(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchStableMergeIDs,
	)
}

func testUserMergeBranchStableMergeIDs(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("Merged by Gitaly"),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}

	// Because the timestamp is
	expectedMergeID := "f0165798887392f9148b55d54a832b005f93a38c"

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	response, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")
	require.Equal(t, response.CommitId, expectedMergeID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	response, err = mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	require.Equal(t, expectedMergeID, response.BranchUpdate.CommitId)

	_, err = mergeBidi.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")
	require.Equal(t, commit, &gitalypb.GitCommit{
		Subject:  []byte("Merged by Gitaly"),
		Body:     []byte("Merged by Gitaly"),
		BodySize: 16,
		Id:       expectedMergeID,
		ParentIds: []string{
			"281d3a76f31c812dbf48abce82ccf6860adedd81",
			"e63f41fe459e62e1228fcef60d7189127aeba95a",
		},
		TreeId: "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
		Author: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	})
}

func TestUserMergeBranch_abort(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchAbort,
	)
}

func testUserMergeBranchAbort(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("foobar"),
	}

	testCases := []struct {
		req       *gitalypb.UserMergeBranchRequest
		closeSend bool
		desc      string
	}{
		{req: &gitalypb.UserMergeBranchRequest{Repository: &gitalypb.Repository{}}, desc: "empty request, don't close"},
		{req: &gitalypb.UserMergeBranchRequest{Repository: &gitalypb.Repository{}}, closeSend: true, desc: "empty request and close"},
		{closeSend: true, desc: "no request just close"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			firstResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "first response")
			require.NotEqual(t, "", firstResponse.CommitId, "commit ID on first response")

			if tc.req != nil {
				require.NoError(t, mergeBidi.Send(tc.req), "send second request")
			}

			if tc.closeSend {
				require.NoError(t, mergeBidi.CloseSend(), "close request stream from client")
			}

			secondResponse, err := mergeBidi.Recv()
			require.Equal(t, "", secondResponse.GetBranchUpdate().GetCommitId(), "merge should not have been applied")
			require.Error(t, err)

			commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
			require.NoError(t, err, "look up git commit after call has finished")

			require.Equal(t, mergeBranchHeadBefore, commit.Id, "branch should not change when the merge is aborted")
		})
	}
}

func TestUserMergeBranch_concurrentUpdate(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchConcurrentUpdate,
	)
}

func testUserMergeBranchConcurrentUpdate(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
		Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	// This concurrent update of the branch we are merging into should make the merge fail.
	concurrentCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(mergeBranchName))
	require.NotEqual(t, firstResponse.CommitId, concurrentCommitID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("Could not update refs/heads/gitaly-merge-test-branch. Please refresh and try again.").WithDetail(
		&gitalypb.UserMergeBranchError{
			Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
				ReferenceUpdate: &gitalypb.ReferenceUpdateError{
					ReferenceName: []byte("refs/heads/" + mergeBranchName),
					OldOid:        "281d3a76f31c812dbf48abce82ccf6860adedd81",
					NewOid:        "f0165798887392f9148b55d54a832b005f93a38c",
				},
			},
		},
	), err)
	require.Nil(t, secondResponse)

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID.String(), "RPC should not have trampled concurrent update")
}

func TestUserMergeBranch_ambiguousReference(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchAmbiguousReference,
	)
}

func testUserMergeBranchAmbiguousReference(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	merge, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	masterOID, err := repo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	// We're now creating all kinds of potentially ambiguous references in
	// the hope that UserMergeBranch won't be confused by it.
	for _, reference := range []string{
		mergeBranchName,
		"heads/" + mergeBranchName,
		"refs/heads/refs/heads/" + mergeBranchName,
		"refs/tags/" + mergeBranchName,
		"refs/tags/heads/" + mergeBranchName,
		"refs/tags/refs/heads/" + mergeBranchName,
	} {
		require.NoError(t, repo.UpdateRef(ctx, git.ReferenceName(reference), masterOID, git.ObjectHashSHA1.ZeroOID))
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, merge.Send(firstRequest), "send first request")

	_, err = merge.Recv()
	require.NoError(t, err, "receive first response")
	require.NoError(t, err, "look up git commit before merge is applied")
	require.NoError(t, merge.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")

	response, err := merge.Recv()
	require.NoError(t, err, "receive second response")

	_, err = merge.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := repo.ReadCommit(ctx, git.Revision("refs/heads/"+mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")

	testhelper.ProtoEqual(t, &gitalypb.OperationBranchUpdate{CommitId: commit.Id}, response.BranchUpdate)
	require.Equal(t, mergeCommitMessage, string(commit.Body))
	require.Equal(t, gittest.TestUser.Name, commit.Author.Name)
	require.Equal(t, gittest.TestUser.Email, commit.Author.Email)
	require.Equal(t, []string{mergeBranchHeadBefore, commitToMerge}, commit.ParentIds)
}

func TestUserMergeBranch_failingHooks(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchFailingHooks,
	)
}

func testUserMergeBranchFailingHooks(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	hookContent := []byte("#!/bin/sh\necho 'stdout' && echo 'stderr' >&2\nexit 1")

	for _, tc := range []struct {
		hookName   string
		hookType   gitalypb.CustomHookError_HookType
		shouldFail bool
	}{
		{
			hookName:   "pre-receive",
			hookType:   gitalypb.CustomHookError_HOOK_TYPE_PRERECEIVE,
			shouldFail: true,
		},
		{
			hookName:   "update",
			hookType:   gitalypb.CustomHookError_HOOK_TYPE_UPDATE,
			shouldFail: true,
		},
		{
			hookName: "post-receive",
			hookType: gitalypb.CustomHookError_HOOK_TYPE_POSTRECEIVE,
			// The post-receive hook runs after references have been updated and any
			// failures of it are ignored.
			shouldFail: false,
		},
	} {
		t.Run(tc.hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, tc.hookName, hookContent)

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &gitalypb.UserMergeBranchRequest{
				Repository: repo,
				User:       gittest.TestUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte(mergeCommitMessage),
			}

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			firstResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "receive first response")

			require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
			require.NoError(t, mergeBidi.CloseSend(), "close send")

			secondResponse, err := mergeBidi.Recv()
			if tc.shouldFail {
				testhelper.RequireGrpcError(t, structerr.NewPermissionDenied("stderr\n").WithDetail(
					&gitalypb.UserMergeBranchError{
						Error: &gitalypb.UserMergeBranchError_CustomHook{
							CustomHook: &gitalypb.CustomHookError{
								HookType: tc.hookType,
								Stdout:   []byte("stdout\n"),
								Stderr:   []byte("stderr\n"),
							},
						},
					},
				), err)
				require.Nil(t, secondResponse)
			} else {
				testhelper.ProtoEqual(t, &gitalypb.UserMergeBranchResponse{
					BranchUpdate: &gitalypb.OperationBranchUpdate{
						CommitId: firstResponse.CommitId,
					},
				}, secondResponse)
				require.NoError(t, err)
			}

			currentBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", mergeBranchName)
			if !tc.shouldFail {
				require.Equal(t, firstResponse.CommitId, text.ChompBytes(currentBranchHead), "branch head updated")
			} else {
				require.Equal(t, mergeBranchHeadBefore, text.ChompBytes(currentBranchHead), "branch head updated")
			}
		})
	}
}

func TestUserMergeBranch_conflict(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchConflict,
	)
}

func testUserMergeBranchConflict(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	const mergeIntoBranch = "mergeIntoBranch"
	const mergeFromBranch = "mergeFromBranch"
	const conflictingFile = "file"

	baseCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeIntoBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data",
	}))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeFromBranch, baseCommit.String())

	divergedInto := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeIntoBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data-1",
	}))

	divergedFrom := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(mergeFromBranch), gittest.WithTreeEntries(gittest.TreeEntry{
		Mode: "100644", Path: conflictingFile, Content: "data-2",
	}))

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Branch:     []byte(mergeIntoBranch),
		CommitId:   divergedFrom.String(),
		Message:    []byte("msg"),
	}), "send first request")

	firstResponse, err := mergeBidi.Recv()
	testhelper.RequireGrpcError(t, structerr.NewFailedPrecondition("merging commits: merge: there are conflicting files").WithDetail(
		&gitalypb.UserMergeBranchError{
			Error: &gitalypb.UserMergeBranchError_MergeConflict{
				MergeConflict: &gitalypb.MergeConflictError{
					ConflictingFiles: [][]byte{
						[]byte(conflictingFile),
					},
					ConflictingCommitIds: []string{
						divergedInto.String(),
						divergedFrom.String(),
					},
				},
			},
		},
	), err)
	require.Nil(t, firstResponse)
}

func TestUserMergeBranch_allowed(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.MergeTreeMerge).Run(
		t,
		testUserMergeBranchAllowed,
	)
}

func testUserMergeBranchAllowed(t *testing.T, ctx context.Context) {
	t.Parallel()

	mergeBranchHeadAfter := "ff0ac4dfa30d6b26fd14aa83a75650355270bf76"

	for _, tc := range []struct {
		desc             string
		allowed          bool
		allowedMessage   string
		allowedErr       error
		expectedErr      error
		expectedResponse *gitalypb.UserMergeBranchResponse
	}{
		{
			desc:    "allowed",
			allowed: true,
			expectedResponse: &gitalypb.UserMergeBranchResponse{
				BranchUpdate: &gitalypb.OperationBranchUpdate{
					CommitId: mergeBranchHeadAfter,
				},
			},
		},
		{
			desc:           "disallowed",
			allowed:        false,
			allowedMessage: "you shall not pass",
			expectedErr: structerr.NewPermissionDenied("GitLab: you shall not pass").WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "you shall not pass",
							Protocol:     "web",
							UserId:       gittest.GlID,
							Changes:      []byte(fmt.Sprintf("%s %s refs/heads/%s\n", mergeBranchHeadBefore, mergeBranchHeadAfter, mergeBranchName)),
						},
					},
				},
			),
		},
		{
			desc:       "failing",
			allowedErr: errors.New("failure"),
			expectedErr: structerr.NewPermissionDenied("GitLab: failure").WithDetail(
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: "failure",
							Protocol:     "web",
							UserId:       gittest.GlID,
							Changes:      []byte(fmt.Sprintf("%s %s refs/heads/%s\n", mergeBranchHeadBefore, mergeBranchHeadAfter, mergeBranchName)),
						},
					},
				},
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)
			backchannelRegistry := backchannel.NewRegistry()
			txManager := transaction.NewManager(cfg, backchannelRegistry)
			hookManager := hook.NewManager(cfg, config.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), txManager, gitlab.NewMockClient(
				t,
				func(context.Context, gitlab.AllowedParams) (bool, string, error) {
					return tc.allowed, tc.allowedMessage, tc.allowedErr
				},
				gitlab.MockPreReceive,
				gitlab.MockPostReceive,
			))

			ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithCfg(
				t, ctx, cfg,
				testserver.WithBackchannelRegistry(backchannelRegistry),
				testserver.WithTransactionManager(txManager),
				testserver.WithHookManager(hookManager),
			)

			gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

			stream, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte("message"),
				Timestamp:  &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
			}))

			response, err := stream.Recv()
			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.UserMergeBranchResponse{
				CommitId: mergeBranchHeadAfter,
			}, response)

			require.NoError(t, stream.Send(&gitalypb.UserMergeBranchRequest{
				Apply: true,
			}))

			response, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponse, response)

			if err == nil {
				_, err = stream.Recv()
				require.Equal(t, io.EOF, err)
			}
		})
	}
}

func errWithDetails(tb testing.TB, err error, details ...proto.Message) error {
	detailedErr := structerr.New("%w", err)
	for _, detail := range details {
		detailedErr = detailedErr.WithDetail(detail)
	}
	return detailedErr
}

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
			expectedErr: structerr.NewInvalidArgument(testhelper.GitalyOrPraefect("empty Repository", "repo scoped: empty Repository")),
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
			expectedErr: structerr.NewInvalidArgument(fmt.Sprintf(`invalid expected old object ID: invalid object ID: "foobar", expected length %v, got 6`, gittest.DefaultObjectHash.EncodedLen())).
				WithInterceptedMetadata("old_object_id", "foobar"),
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
			expectedErr: structerr.NewInvalidArgument("cannot resolve expected old object ID: reference not found").
				WithInterceptedMetadata("old_object_id", gittest.DefaultObjectHash.ZeroOID),
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

func TestUserMergeToRef_successful(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	existingTargetRef := []byte("refs/merge-requests/x/written")
	emptyTargetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	// Writes in existingTargetRef
	beforeRefreshCommitSha := "a5391128b0ef5d21df5dd23d98557f4ef12fae20"
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", string(existingTargetRef), beforeRefreshCommitSha)

	testCases := []struct {
		desc           string
		user           *gitalypb.User
		branch         []byte
		targetRef      []byte
		emptyRef       bool
		sourceSha      string
		message        string
		firstParentRef []byte
	}{
		{
			desc:           "empty target ref merge",
			user:           gittest.TestUser,
			targetRef:      emptyTargetRef,
			emptyRef:       true,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:           "existing target ref",
			user:           gittest.TestUser,
			targetRef:      existingTargetRef,
			emptyRef:       false,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:      "branch is specified and firstParentRef is empty",
			user:      gittest.TestUser,
			branch:    []byte(mergeBranchName),
			targetRef: existingTargetRef,
			emptyRef:  false,
			sourceSha: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
			message:   mergeCommitMessage,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserMergeToRefRequest{
				Repository:     repoProto,
				User:           testCase.user,
				Branch:         testCase.branch,
				TargetRef:      testCase.targetRef,
				SourceSha:      testCase.sourceSha,
				Message:        []byte(testCase.message),
				FirstParentRef: testCase.firstParentRef,
			}

			commitBeforeRefMerge, fetchRefBeforeMergeErr := repo.ReadCommit(ctx, git.Revision(testCase.targetRef))
			if testCase.emptyRef {
				require.Error(t, fetchRefBeforeMergeErr, "error when fetching empty ref commit")
			} else {
				require.NoError(t, fetchRefBeforeMergeErr, "no error when fetching existing ref")
			}

			resp, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)

			commit, err := repo.ReadCommit(ctx, git.Revision(testCase.targetRef))
			require.NoError(t, err, "look up git commit after call has finished")

			// Asserts commit parent SHAs
			require.Equal(t, []string{mergeBranchHeadBefore, testCase.sourceSha}, commit.ParentIds, "merge commit parents must be the sha before HEAD and source sha")

			require.True(t, strings.HasPrefix(string(commit.Body), testCase.message), "expected %q to start with %q", commit.Body, testCase.message)

			// Asserts author
			author := commit.Author
			require.Equal(t, gittest.TestUser.Name, author.Name)
			require.Equal(t, gittest.TestUser.Email, author.Email)
			require.Equal(t, gittest.TimezoneOffset, string(author.Timezone))

			require.Equal(t, resp.CommitId, commit.Id)

			// Calling commitBeforeRefMerge.Id in a non-existent
			// commit will raise a null-pointer error.
			if !testCase.emptyRef {
				require.NotEqual(t, commit.Id, commitBeforeRefMerge.Id)
			}
		})
	}
}

func TestUserMergeToRef_conflicts(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("allow conflicts to be merged with markers when modified on both sides", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", "modified-both-sides-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< files\/ruby\/popen.rb.*?\+>>>>>>> files\/ruby\/popen.rb.*?\+<<<<<<< files\/ruby\/regex.rb.*?\+>>>>>>> files\/ruby\/regex.rb`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when modified on source and removed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "eb227b3e214624708c474bdab7bde7afc17cefcc", "92417abf83b75e67b8ace920bc8e83e1986da4ac", "modified-source-removed-target-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< \n.*?\+=======\n.*?\+>>>>>>> files/ruby/version_info.rb`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when removed on source and modified on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "92417abf83b75e67b8ace920bc8e83e1986da4ac", "eb227b3e214624708c474bdab7bde7afc17cefcc", "removed-source-modified-target-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< files/ruby/version_info.rb.*?\+=======\n.*?\+>>>>>>> \z`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when both source and target added the same file", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "f0f390655872bb2772c85a0128b2fbc2d88670cb", "5b4bb08538b9249995b94aa69121365ba9d28082", "source-target-added-same-file-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< NEW_FILE.md.*?\+=======\n.*?\+>>>>>>> NEW_FILE.md`)
		require.Regexp(t, markersRegexp, output)
	})

	// Test cases below do not show any conflict markers because we don't try
	// to merge the conflicts for these cases. We keep `Their` side of the
	// conflict and ignore `Our` and `Ancestor` instead. This is because we want
	// to show the `Their` side when we present the merge commit on the merge
	// request diff.
	t.Run("allow conflicts to be merged without markers when renamed on source and removed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "3ac7abfb7621914e596d5bf369be8234b9086052", "renamed-source-removed-target")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("allow conflicts to be merged without markers when removed on source and renamed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "3ac7abfb7621914e596d5bf369be8234b9086052", "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "removed-source-renamed-target")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("allow conflicts to be merged without markers when both source and target renamed the same file", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "fe6d6ff5812e7fb292168851dc0edfc6a0171909", "source-target-renamed-same-file")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("disallow conflicts to be merged", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", "disallowed-conflicts")
		request.AllowConflicts = false

		_, err := client.UserMergeToRef(ctx, request)
		testhelper.RequireGrpcError(t, status.Error(codes.FailedPrecondition, "Failed to create merge commit for source_sha 1450cd639e0bc6721eb02800169e464f212cde06 and target_sha 824be604a34828eb682305f0d963056cfac87b2d at refs/merge-requests/x/written"), err)
	})

	targetRef := git.Revision("refs/merge-requests/foo")

	t.Run("failing merge does not update target reference if skipping precursor update-ref", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", t.Name())
		request.TargetRef = []byte(targetRef)

		_, err := client.UserMergeToRef(ctx, request)
		testhelper.RequireGrpcCode(t, err, codes.FailedPrecondition)

		hasRevision, err := repo.HasRevision(ctx, targetRef)
		require.NoError(t, err)
		require.False(t, hasRevision, "branch should not have been created")
	})
}

func buildUserMergeToRefRequest(tb testing.TB, cfg config.Cfg, repo *gitalypb.Repository, repoPath string, sourceSha string, targetSha string, mergeBranchName string) *gitalypb.UserMergeToRefRequest {
	gittest.Exec(tb, cfg, "-C", repoPath, "branch", mergeBranchName, targetSha)

	return &gitalypb.UserMergeToRefRequest{
		Repository:     repo,
		User:           gittest.TestUser,
		TargetRef:      []byte("refs/merge-requests/x/written"),
		SourceSha:      sourceSha,
		Message:        []byte("message1"),
		FirstParentRef: []byte("refs/heads/" + mergeBranchName),
	}
}

func TestUserMergeToRef_stableMergeID(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	response, err := client.UserMergeToRef(ctx, &gitalypb.UserMergeToRefRequest{
		Repository:     repoProto,
		User:           gittest.TestUser,
		FirstParentRef: []byte("refs/heads/" + mergeBranchName),
		TargetRef:      []byte("refs/merge-requests/x/written"),
		SourceSha:      "1450cd639e0bc6721eb02800169e464f212cde06",
		Message:        []byte("Merge message"),
		Timestamp:      &timestamppb.Timestamp{Seconds: 12, Nanos: 34},
	})
	require.NoError(t, err)
	require.Equal(t, "c7b65194ce2da804557582408ab94713983d0b70", response.CommitId)

	commit, err := repo.ReadCommit(ctx, git.Revision("refs/merge-requests/x/written"))
	require.NoError(t, err, "look up git commit after call has finished")
	testhelper.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:  []byte("Merge message"),
		Body:     []byte("Merge message"),
		BodySize: 13,
		Id:       "c7b65194ce2da804557582408ab94713983d0b70",
		ParentIds: []string{
			"281d3a76f31c812dbf48abce82ccf6860adedd81",
			"1450cd639e0bc6721eb02800169e464f212cde06",
		},
		TreeId: "3d3c2dd807abaf36d7bd5334bf3f8c5cf61bad75",
		Author: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamppb.Timestamp{Seconds: 12},
			Timezone: []byte(gittest.TimezoneOffset),
		},
	}, commit)
}

func TestUserMergeToRef_failure(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	validTargetRef := []byte("refs/merge-requests/x/merge")

	testCases := []struct {
		desc      string
		user      *gitalypb.User
		branch    []byte
		targetRef []byte
		sourceSha string
		repo      *gitalypb.Repository
		code      codes.Code
	}{
		{
			desc:      "empty repository",
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty user",
			repo:      repo,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty source SHA",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing commit",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: "f001",
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty branch and first parent ref",
			repo:      repo,
			user:      gittest.TestUser,
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "invalid target ref",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: []byte("refs/heads/branch"),
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing branch",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte("this-isnt-real"),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserMergeToRefRequest{
				Repository: testCase.repo,
				User:       testCase.user,
				Branch:     testCase.branch,
				SourceSha:  testCase.sourceSha,
				TargetRef:  testCase.targetRef,
			}
			_, err := client.UserMergeToRef(ctx, request)
			testhelper.RequireGrpcCode(t, err, testCase.code)
		})
	}
}

func TestUserMergeToRef_ignoreHooksRequest(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	targetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	request := &gitalypb.UserMergeToRefRequest{
		Repository: repo,
		SourceSha:  commitToMerge,
		Branch:     []byte(mergeBranchName),
		TargetRef:  targetRef,
		User:       gittest.TestUser,
		Message:    []byte(mergeCommitMessage),
	}

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			_, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)
		})
	}
}
