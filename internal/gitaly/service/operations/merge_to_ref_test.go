//go:build !gitaly_test_sha256

package operations

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUserMergeToRef_successful(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
		featureflag.MergeToRefWithGit,
	).Run(
		t,
		testUserMergeToRefSuccessful,
	)
}

func testUserMergeToRefSuccessful(t *testing.T, ctx context.Context) {
	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	branch := "main"
	firstParentRef := "refs/heads/" + branch
	branchSha := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithReference(firstParentRef),
		gittest.WithMessage("branch commit"),
	).String()
	sourceSha := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("source-branch"),
		gittest.WithMessage("source branch commit"),
	).String()
	existingTargetRef := []byte("refs/merge-requests/x/written")
	existingTargetRefOid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(string(existingTargetRef)))
	emptyTargetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	testCases := []struct {
		desc           string
		user           *gitalypb.User
		branch         []byte
		targetRef      []byte
		emptyRef       bool
		sourceSha      string
		message        string
		firstParentRef []byte
		expectedOldOid string
	}{
		{
			desc:           "empty target ref merge",
			user:           gittest.TestUser,
			targetRef:      emptyTargetRef,
			emptyRef:       true,
			sourceSha:      sourceSha,
			message:        mergeCommitMessage,
			firstParentRef: []byte(firstParentRef),
		},
		{
			desc:           "existing target ref",
			user:           gittest.TestUser,
			targetRef:      existingTargetRef,
			emptyRef:       false,
			sourceSha:      sourceSha,
			message:        mergeCommitMessage,
			firstParentRef: []byte(firstParentRef),
		},
		{
			desc:           "existing target ref with optimistic lock",
			user:           gittest.TestUser,
			targetRef:      existingTargetRef,
			emptyRef:       false,
			sourceSha:      sourceSha,
			message:        mergeCommitMessage,
			firstParentRef: []byte(firstParentRef),
			expectedOldOid: existingTargetRefOid.String(),
		},
		{
			desc:      "branch is specified and firstParentRef is empty",
			user:      gittest.TestUser,
			branch:    []byte(branch),
			targetRef: existingTargetRef,
			emptyRef:  false,
			sourceSha: sourceSha,
			message:   mergeCommitMessage,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			// reset target ref
			gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(existingTargetRef), existingTargetRefOid)

			request := &gitalypb.UserMergeToRefRequest{
				Repository:     repoProto,
				User:           testCase.user,
				Branch:         testCase.branch,
				TargetRef:      testCase.targetRef,
				SourceSha:      testCase.sourceSha,
				Message:        []byte(testCase.message),
				FirstParentRef: testCase.firstParentRef,
				ExpectedOldOid: testCase.expectedOldOid,
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
			require.Equal(t, []string{branchSha, testCase.sourceSha}, commit.ParentIds, "merge commit parents must be the sha before HEAD and source sha")

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

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
		featureflag.MergeToRefWithGit,
	).Run(
		t,
		testUserMergeToRefConflicts,
	)
}

func testUserMergeToRefConflicts(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("disallow conflicts to be merged", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", "disallowed-conflicts")

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

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
		featureflag.MergeToRefWithGit,
	).Run(
		t,
		testUserMergeToRefStableMergeID,
	)
}

func testUserMergeToRefStableMergeID(t *testing.T, ctx context.Context) {
	t.Parallel()

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
	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
		featureflag.MergeToRefWithGit,
	).Run(t, testUserMergeToRefFailure)
}

func testUserMergeToRefFailure(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, client := setupOperationsServiceWithoutRepo(t, ctx)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)

	validBranchName := "main"
	gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(validBranchName),
		gittest.WithMessage("branch commit"),
	)
	validSourceSha := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("source-branch"),
		gittest.WithMessage("source branch commit"),
	).String()
	validTargetRef := []byte("refs/merge-requests/x/merge")

	testCases := []struct {
		desc           string
		user           *gitalypb.User
		branch         []byte
		targetRef      []byte
		sourceSha      string
		repo           *gitalypb.Repository
		code           codes.Code
		expectedOldOid string
		message        []byte
	}{
		{
			desc:      "empty repository",
			user:      gittest.TestUser,
			branch:    []byte(validBranchName),
			sourceSha: validSourceSha,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty user",
			repo:      repoProto,
			branch:    []byte(validBranchName),
			sourceSha: validSourceSha,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty source SHA",
			repo:      repoProto,
			user:      gittest.TestUser,
			branch:    []byte(validBranchName),
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing commit",
			repo:      repoProto,
			user:      gittest.TestUser,
			branch:    []byte(validBranchName),
			sourceSha: "f001",
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty branch and first parent ref",
			repo:      repoProto,
			user:      gittest.TestUser,
			sourceSha: validSourceSha,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "invalid target ref",
			repo:      repoProto,
			user:      gittest.TestUser,
			branch:    []byte(validBranchName),
			sourceSha: validSourceSha,
			targetRef: []byte("refs/heads/branch"),
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing branch",
			repo:      repoProto,
			user:      gittest.TestUser,
			branch:    []byte("this-isnt-real"),
			sourceSha: validSourceSha,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:           "non-matching expected_object_id",
			repo:           repoProto,
			user:           gittest.TestUser,
			branch:         []byte(validBranchName),
			sourceSha:      validSourceSha,
			targetRef:      validTargetRef,
			code:           codes.FailedPrecondition,
			message:        []byte("some merge commit message"),
			expectedOldOid: validSourceSha, // arbitrary value that differs from current target ref OID
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserMergeToRefRequest{
				Repository:     testCase.repo,
				User:           testCase.user,
				Branch:         testCase.branch,
				SourceSha:      testCase.sourceSha,
				TargetRef:      testCase.targetRef,
				Message:        testCase.message,
				ExpectedOldOid: testCase.expectedOldOid,
			}
			_, err := client.UserMergeToRef(ctx, request)
			testhelper.RequireGrpcCode(t, err, testCase.code)
		})
	}
}

func TestUserMergeToRef_ignoreHooksRequest(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(
		featureflag.GPGSigning,
		featureflag.MergeToRefWithGit,
	).Run(
		t,
		testUserMergeToRefIgnoreHooksRequest,
	)
}

func testUserMergeToRefIgnoreHooksRequest(t *testing.T, ctx context.Context) {
	t.Parallel()

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
