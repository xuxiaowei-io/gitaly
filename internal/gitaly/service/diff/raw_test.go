//go:build !gitaly_test_sha256

package diff

import (
	"fmt"
	"io"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc/codes"
)

func TestRawDiff_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffServiceWithoutRepo(t)
	testcfg.BuildGitalyGit2Go(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg)
	oldBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("old\n"))
	newBlob := gittest.WriteBlob(t, cfg, repoPath, []byte("new\n"))
	leftCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: oldBlob},
	))
	rightCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: newBlob},
	))

	stream, err := client.RawDiff(ctx, &gitalypb.RawDiffRequest{
		Repository:    repoProto,
		LeftCommitId:  leftCommit.String(),
		RightCommitId: rightCommit.String(),
	})
	require.NoError(t, err)

	rawDiff, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	}))
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf(`diff --git a/file b/file
index %s..%s 100644
--- a/file
+++ b/file
@@ -1 +1 @@
-old
+new
`, oldBlob, newBlob), string(rawDiff))
}

func TestRawDiff_inputValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupDiffServiceWithoutRepo(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	commit := gittest.WriteCommit(t, cfg, repoPath)

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.RawDiffRequest
		expectedErr error
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("empty LeftCommitId"),
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				LeftCommitId:  commit.String(),
				RightCommitId: "",
			},
			expectedErr: structerr.NewInvalidArgument("empty RightCommitId"),
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawDiffRequest{
				Repository:    nil,
				RightCommitId: commit.String(),
				LeftCommitId:  commit.String(),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.RawDiff(ctx, tc.request)
			require.NoError(t, err)
			testhelper.RequireGrpcError(t, tc.expectedErr, drainRawDiffResponse(stream))
		})
	}
}

func TestRawPatch_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, client := setupDiffService(t, ctx)
	testcfg.BuildGitalyGit2Go(t, cfg)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	locator := config.NewLocator(cfg)
	git2goExecutor := git2go.NewExecutor(cfg, gitCmdFactory, locator)

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"

	stream, err := client.RawPatch(ctx, &gitalypb.RawPatchRequest{
		Repository:    repoProto,
		LeftCommitId:  leftCommit,
		RightCommitId: rightCommit,
	})
	require.NoError(t, err)

	rawPatch, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	}))
	require.NoError(t, err)

	signature := git2go.Signature{
		Name:  gittest.DefaultCommitterName,
		Email: gittest.DefaultCommitterMail,
		When:  gittest.DefaultCommitTime,
	}

	// Now that we have read the patch in we verify that it indeed round-trips to the same tree
	// as the right commit is referring to by reapplying the diff on top of the left commit.
	patchedCommitID, err := git2goExecutor.Apply(ctx, gittest.RewrittenRepository(t, ctx, cfg, repoProto), git2go.ApplyParams{
		Repository:   repoPath,
		Committer:    signature,
		ParentCommit: leftCommit,
		Patches: git2go.NewSlicePatchIterator([]git2go.Patch{{
			Author:  signature,
			Message: "Applying received raw patch",
			Diff:    rawPatch,
		}}),
	})
	require.NoError(t, err)

	// Peel both right commit and patched commit to their trees and assert that they refer to
	// the same one.
	require.Equal(t,
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", rightCommit+"^{tree}"),
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", patchedCommitID.String()+"^{tree}"),
	)
}

func TestRawPatch_inputValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	testCases := []struct {
		desc    string
		request *gitalypb.RawPatchRequest
		code    codes.Code
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				RightCommitId: "",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawPatchRequest{
				Repository:    nil,
				RightCommitId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			c, _ := client.RawPatch(ctx, testCase.request)
			testhelper.RequireGrpcCode(t, drainRawPatchResponse(c), testCase.code)
		})
	}
}

func TestRawPatch_gitlabSignature(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupDiffService(t, ctx)

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.RawPatchRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawPatch(ctx, rpcRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	patch, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Regexp(t, regexp.MustCompile(`\n-- \nGitLab\s+$`), string(patch))
}

func drainRawDiffResponse(c gitalypb.DiffService_RawDiffClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}

func drainRawPatchResponse(c gitalypb.DiffService_RawPatchClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
