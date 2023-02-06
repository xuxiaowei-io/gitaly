//go:build !gitaly_test_sha256

package repository

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulCalculateChecksum(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoPath, d), perm.SharedDir))
	}

	testhelper.CopyFile(t, "testdata/checksum-test-packed-refs", filepath.Join(repoPath, "packed-refs"))
	gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}

	response, err := client.CalculateChecksum(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "0c500d7c8a9dbf65e4cf5e58914bec45bfb6e9ab", response.Checksum)
}

func TestEmptyRepositoryCalculateChecksum(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}

	response, err := client.CalculateChecksum(ctx, request)
	require.NoError(t, err)
	require.Equal(t, git.ZeroChecksum, response.Checksum)
}

func TestBrokenRepositoryCalculateChecksum(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	// Force an empty HEAD file
	require.NoError(t, os.Truncate(filepath.Join(repoPath, "HEAD"), 0))

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}

	_, err := client.CalculateChecksum(ctx, request)
	testhelper.RequireGrpcCode(t, err, codes.DataLoss)
}

func TestFailedCalculateChecksum(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc        string
		request     *gitalypb.CalculateChecksumRequest
		expectedErr error
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.CalculateChecksumRequest{Repository: invalidRepo},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				`GetStorageByName: no such storage: "fake"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.CalculateChecksumRequest{},
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefect(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
	}

	for _, testCase := range testCases {
		testCtx := testhelper.Context(t)

		_, err := client.CalculateChecksum(testCtx, testCase.request)
		testhelper.RequireGrpcError(t, testCase.expectedErr, err)
	}
}

func TestInvalidRefsCalculateChecksum(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoPath, d), perm.SharedDir))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-invalid-refs", filepath.Join(repoPath, "packed-refs")).Run())

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}

	response, err := client.CalculateChecksum(ctx, request)
	require.NoError(t, err)
	require.Equal(t, git.ZeroChecksum, response.Checksum)
}
