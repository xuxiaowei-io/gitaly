//go:build !gitaly_test_sha256

package stats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
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

func TestLogObjectInfo(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repo1, repoPath1 := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})
	repo2, repoPath2 := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   gittest.SeedGitLabTest,
	})

	requireLog := func(entries []*logrus.Entry) map[string]interface{} {
		for _, entry := range entries {
			if entry.Message == "git repo statistic" {
				const key = "count_objects"
				data := entry.Data[key]
				require.NotNil(t, data, "there is no any information about statistics")
				countObjects, ok := data.(map[string]interface{})
				require.True(t, ok)
				require.Contains(t, countObjects, "count")
				require.Contains(t, countObjects, "size")
				require.Contains(t, countObjects, "in-pack")
				require.Contains(t, countObjects, "packs")
				require.Contains(t, countObjects, "size-pack")
				require.Contains(t, countObjects, "garbage")
				require.Contains(t, countObjects, "size-garbage")
				return countObjects
			}
		}
		return nil
	}

	t.Run("shared repo with multiple alternates", func(t *testing.T) {
		locator := config.NewLocator(cfg)
		storagePath, err := locator.GetStorageByName(repo1.GetStorageName())
		require.NoError(t, err)

		tmpDir, err := os.MkdirTemp(storagePath, "")
		require.NoError(t, err)
		defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

		// clone existing local repo with two alternates
		gittest.Exec(t, cfg, "clone", "--shared", repoPath1, "--reference", repoPath1, "--reference", repoPath2, tmpDir)

		logger, hook := test.NewNullLogger()
		testCtx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		LogObjectsInfo(testCtx, localrepo.NewTestRepo(t, cfg, &gitalypb.Repository{
			StorageName:  repo1.StorageName,
			RelativePath: filepath.Join(strings.TrimPrefix(tmpDir, storagePath), ".git"),
		}))

		countObjects := requireLog(hook.AllEntries())
		require.ElementsMatch(t, []string{repoPath1 + "/objects", repoPath2 + "/objects"}, countObjects["alternate"])
	})

	t.Run("repo without alternates", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		testCtx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		LogObjectsInfo(testCtx, localrepo.NewTestRepo(t, cfg, repo2))

		countObjects := requireLog(hook.AllEntries())
		require.Contains(t, countObjects, "prune-packable")
	})
}
