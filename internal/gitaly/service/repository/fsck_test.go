//go:build !gitaly_test_sha256

package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFsckSuccess(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRepositoryService(t, ctx)

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Empty(t, c.GetError())
}

func TestFsckFailureSeverelyBrokenRepo(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	// This makes the repo severely broken so that `git` does not identify it as a
	// proper repo.
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects")))
	fd, err := os.Create(filepath.Join(repoPath, "objects"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Contains(t, strings.ToLower(string(c.GetError())), "not a git repository")
}

func TestFsckFailureSlightlyBrokenRepo(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	// This makes the repo slightly broken so that `git` still identify it as a
	// proper repo, but `fsck` complains about broken refs...
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects", "pack")))

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotEmpty(t, string(c.GetError()))
	assert.Contains(t, string(c.GetError()), "error: HEAD: invalid sha1 pointer")
}

func TestFsck_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, client := setupRepositoryServiceWithoutRepo(t)

	_, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: nil})
	msg := testhelper.GitalyOrPraefectMessage("empty Repository", "repo scoped: empty Repository")
	testhelper.RequireGrpcError(t, status.Error(codes.InvalidArgument, msg), err)
}
