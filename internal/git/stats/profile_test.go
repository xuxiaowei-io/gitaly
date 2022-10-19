//go:build !gitaly_test_sha256

package stats

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestRepositoryProfile(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	hasBitmap, err := HasBitmap(repoPath)
	require.NoError(t, err)
	require.False(t, hasBitmap, "repository should not have a bitmap initially")
	packfiles, err := GetPackfiles(repoPath)
	require.NoError(t, err)
	require.Empty(t, packfiles)
	packfilesCount, err := PackfilesCount(repoPath)
	require.NoError(t, err)
	require.Zero(t, packfilesCount)

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, cfg, repoPath, blobs)

	looseObjects, err := LooseObjects(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, int64(blobs), looseObjects)

	for _, blobID := range blobIDs {
		commitID := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithTreeEntries(gittest.TreeEntry{
				Mode: "100644", Path: "blob", OID: git.ObjectID(blobID),
			}),
		)
		gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/"+blobID, commitID.String())
	}

	// write a loose object
	gittest.WriteBlobs(t, cfg, repoPath, 1)

	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-b", "-d")

	looseObjects, err = LooseObjects(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, int64(1), looseObjects)

	// write another loose object
	blobID := gittest.WriteBlobs(t, cfg, repoPath, 1)[0]

	// due to OS semantics, ensure that the blob has a timestamp that is after the packfile
	theFuture := time.Now().Add(10 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(repoPath, "objects", blobID[0:2], blobID[2:]), theFuture, theFuture))

	looseObjects, err = LooseObjects(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, int64(2), looseObjects)
}
