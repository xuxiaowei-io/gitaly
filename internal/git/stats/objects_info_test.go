//go:build !gitaly_test_sha256

package stats

import (
	"os"
	"path/filepath"
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
	require.Equal(t, uint64(blobs), looseObjects)

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
	require.Equal(t, uint64(1), looseObjects)

	// write another loose object
	blobID := gittest.WriteBlobs(t, cfg, repoPath, 1)[0]

	// due to OS semantics, ensure that the blob has a timestamp that is after the packfile
	theFuture := time.Now().Add(10 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(repoPath, "objects", blobID[0:2], blobID[2:]), theFuture, theFuture))

	looseObjects, err = LooseObjects(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, uint64(2), looseObjects)
}

func TestLogObjectInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	locator := config.NewLocator(cfg)
	storagePath, err := locator.GetStorageByName(cfg.Storages[0].Name)
	require.NoError(t, err)

	requireObjectsInfo := func(entries []*logrus.Entry) ObjectsInfo {
		for _, entry := range entries {
			if entry.Message == "repository objects info" {
				objectsInfo, ok := entry.Data["objects_info"]
				require.True(t, ok)
				require.IsType(t, ObjectsInfo{}, objectsInfo)
				return objectsInfo.(ObjectsInfo)
			}
		}

		require.FailNow(t, "no objects info log entry found")
		return ObjectsInfo{}
	}

	t.Run("shared repo with multiple alternates", func(t *testing.T) {
		t.Parallel()

		logger, hook := test.NewNullLogger()
		ctx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		_, repoPath1 := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		gittest.WriteCommit(t, cfg, repoPath1, gittest.WithMessage("repo1"), gittest.WithBranch("main"))

		_, repoPath2 := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		gittest.WriteCommit(t, cfg, repoPath2, gittest.WithMessage("repo2"), gittest.WithBranch("main"))

		// clone existing local repo with two alternates
		targetRepoName := gittest.NewRepositoryName(t, true)
		targetRepoPath := filepath.Join(storagePath, targetRepoName)
		gittest.Exec(t, cfg, "clone", "--bare", "--shared", repoPath1, "--reference", repoPath1, "--reference", repoPath2, targetRepoPath)

		LogObjectsInfo(ctx, localrepo.NewTestRepo(t, cfg, &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: targetRepoName,
		}))

		objectsInfo := requireObjectsInfo(hook.AllEntries())
		require.Equal(t, ObjectsInfo{
			Alternates: []string{
				filepath.Join(repoPath1, "/objects"),
				filepath.Join(repoPath2, "/objects"),
			},
		}, objectsInfo)
	})

	t.Run("repo without alternates", func(t *testing.T) {
		t.Parallel()

		logger, hook := test.NewNullLogger()
		ctx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

		LogObjectsInfo(ctx, localrepo.NewTestRepo(t, cfg, repo))

		objectsInfo := requireObjectsInfo(hook.AllEntries())
		require.Equal(t, ObjectsInfo{
			LooseObjects:     2,
			LooseObjectsSize: 8,
		}, objectsInfo)
	})
}

func TestObjectsInfoForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	_, alternatePath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	alternatePath = filepath.Join(alternatePath, "objects")

	for _, tc := range []struct {
		desc                string
		setup               func(t *testing.T, repoPath string)
		expectedErr         error
		expectedObjectsInfo ObjectsInfo
	}{
		{
			desc: "empty repository",
			setup: func(*testing.T, string) {
			},
		},
		{
			desc: "single blob",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteBlob(t, cfg, repoPath, []byte("x"))
			},
			expectedObjectsInfo: ObjectsInfo{
				LooseObjects:     1,
				LooseObjectsSize: 4,
			},
		},
		{
			desc: "single packed blob",
			setup: func(t *testing.T, repoPath string) {
				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("x"))
				gittest.WriteRef(t, cfg, repoPath, "refs/tags/blob", blobID)
				// We use `-d`, which also prunes objects that have been packed.
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")
			},
			expectedObjectsInfo: ObjectsInfo{
				PackedObjects: 1,
				Packfiles:     1,
				PackfilesSize: 1,
			},
		},
		{
			desc: "single pruneable blob",
			setup: func(t *testing.T, repoPath string) {
				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("x"))
				gittest.WriteRef(t, cfg, repoPath, "refs/tags/blob", blobID)
				// This time we don't use `-d`, so the object will exist both in
				// loose and packed form.
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-a")
			},
			expectedObjectsInfo: ObjectsInfo{
				LooseObjects:     1,
				LooseObjectsSize: 4,
				PackedObjects:    1,
				Packfiles:        1,
				PackfilesSize:    1,
				PruneableObjects: 1,
			},
		},
		{
			desc: "garbage",
			setup: func(t *testing.T, repoPath string) {
				garbagePath := filepath.Join(repoPath, "objects", "pack", "garbage")
				require.NoError(t, os.WriteFile(garbagePath, []byte("x"), 0o600))
			},
			expectedObjectsInfo: ObjectsInfo{
				Garbage: 1,
				// git-count-objects(1) somehow does not count this file's size,
				// which I've verified manually.
				GarbageSize: 0,
			},
		},
		{
			desc: "alternates",
			setup: func(t *testing.T, repoPath string) {
				infoAlternatesPath := filepath.Join(repoPath, "objects", "info", "alternates")
				require.NoError(t, os.WriteFile(infoAlternatesPath, []byte(alternatePath), 0o600))
			},
			expectedObjectsInfo: ObjectsInfo{
				Alternates: []string{
					alternatePath,
				},
			},
		},
		{
			desc: "all together",
			setup: func(t *testing.T, repoPath string) {
				infoAlternatesPath := filepath.Join(repoPath, "objects", "info", "alternates")
				require.NoError(t, os.WriteFile(infoAlternatesPath, []byte(alternatePath), 0o600))

				// We write a single packed blob.
				blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("x"))
				gittest.WriteRef(t, cfg, repoPath, "refs/tags/blob", blobID)
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad")

				// And two loose ones.
				gittest.WriteBlob(t, cfg, repoPath, []byte("1"))
				gittest.WriteBlob(t, cfg, repoPath, []byte("2"))

				// And three garbage-files. This is done so we've got unique counts
				// everywhere.
				for _, file := range []string{"garbage1", "garbage2", "garbage3"} {
					garbagePath := filepath.Join(repoPath, "objects", "pack", file)
					require.NoError(t, os.WriteFile(garbagePath, []byte("x"), 0o600))
				}
			},
			expectedObjectsInfo: ObjectsInfo{
				LooseObjects:     2,
				LooseObjectsSize: 8,
				PackedObjects:    1,
				Packfiles:        1,
				PackfilesSize:    1,
				Garbage:          3,
				Alternates: []string{
					alternatePath,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.setup(t, repoPath)

			objectsInfo, err := ObjectsInfoForRepository(ctx, repo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedObjectsInfo, objectsInfo)
		})
	}
}
