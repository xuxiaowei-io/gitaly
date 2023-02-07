package stats

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
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
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/perm"
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

	// Assert that the repository is an empty repository that ain't got any packfiles, bitmaps
	// or anything else.
	packfilesInfo, err := PackfilesInfoForRepository(repo)
	require.NoError(t, err)
	require.Equal(t, PackfilesInfo{}, packfilesInfo)

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, cfg, repoPath, blobs)

	looseObjects, err := LooseObjects(repo)
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

	looseObjects, err = LooseObjects(repo)
	require.NoError(t, err)
	require.Equal(t, uint64(1), looseObjects)

	// write another loose object
	blobID := gittest.WriteBlobs(t, cfg, repoPath, 1)[0]

	// due to OS semantics, ensure that the blob has a timestamp that is after the packfile
	theFuture := time.Now().Add(10 * time.Minute)
	require.NoError(t, os.Chtimes(filepath.Join(repoPath, "objects", blobID[0:2], blobID[2:]), theFuture, theFuture))

	looseObjects, err = LooseObjects(repo)
	require.NoError(t, err)
	require.EqualValues(t, 2, looseObjects)
}

func TestLogObjectInfo(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	locator := config.NewLocator(cfg)
	storagePath, err := locator.GetStorageByName(cfg.Storages[0].Name)
	require.NoError(t, err)

	requireRepositoryInfo := func(entries []*logrus.Entry) RepositoryInfo {
		for _, entry := range entries {
			if entry.Message == "repository info" {
				repoInfo, ok := entry.Data["repository_info"]
				require.True(t, ok)
				require.IsType(t, RepositoryInfo{}, repoInfo)
				return repoInfo.(RepositoryInfo)
			}
		}

		require.FailNow(t, "no objects info log entry found")
		return RepositoryInfo{}
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
		targetRepoName := gittest.NewRepositoryName(t)
		targetRepoPath := filepath.Join(storagePath, targetRepoName)
		gittest.Exec(t, cfg, "clone", "--bare", "--shared", repoPath1, "--reference", repoPath1, "--reference", repoPath2, targetRepoPath)

		LogRepositoryInfo(ctx, localrepo.NewTestRepo(t, cfg, &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: targetRepoName,
		}))

		packedRefsStat, err := os.Stat(filepath.Join(targetRepoPath, "packed-refs"))
		require.NoError(t, err)

		repoInfo := requireRepositoryInfo(hook.AllEntries())
		require.Equal(t, RepositoryInfo{
			References: ReferencesInfo{
				PackedReferencesSize: uint64(packedRefsStat.Size()),
			},
			Alternates: []string{
				filepath.Join(repoPath1, "/objects"),
				filepath.Join(repoPath2, "/objects"),
			},
		}, repoInfo)
	})

	t.Run("repo without alternates", func(t *testing.T) {
		t.Parallel()

		logger, hook := test.NewNullLogger()
		ctx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

		LogRepositoryInfo(ctx, localrepo.NewTestRepo(t, cfg, repo))

		objectsInfo := requireRepositoryInfo(hook.AllEntries())
		require.Equal(t, RepositoryInfo{
			LooseObjects: LooseObjectsInfo{
				Count: 2,
				Size:  hashDependentSize(142, 158),
			},
			References: ReferencesInfo{
				LooseReferencesCount: 1,
			},
		}, objectsInfo)
	})
}

func TestRepositoryInfoForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	_, alternatePath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	alternatePath = filepath.Join(alternatePath, "objects")

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T, repoPath string)
		expectedErr  error
		expectedInfo RepositoryInfo
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
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 1,
					Size:  16,
				},
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
			expectedInfo: RepositoryInfo{
				Packfiles: PackfilesInfo{
					Count: 1,
					Size:  hashDependentSize(42, 54),
					Bitmap: BitmapInfo{
						Exists:       true,
						Version:      1,
						HasHashCache: true,
					},
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
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
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 1,
					Size:  16,
				},
				Packfiles: PackfilesInfo{
					Count: 1,
					Size:  hashDependentSize(42, 54),
					Bitmap: BitmapInfo{
						Exists:       true,
						Version:      1,
						HasHashCache: true,
					},
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
			},
		},
		{
			desc: "garbage",
			setup: func(t *testing.T, repoPath string) {
				garbagePath := filepath.Join(repoPath, "objects", "pack", "garbage")
				require.NoError(t, os.WriteFile(garbagePath, []byte("x"), perm.PrivateFile))
			},
			expectedInfo: RepositoryInfo{
				Packfiles: PackfilesInfo{
					GarbageCount: 1,
					GarbageSize:  1,
				},
			},
		},
		{
			desc: "alternates",
			setup: func(t *testing.T, repoPath string) {
				infoAlternatesPath := filepath.Join(repoPath, "objects", "info", "alternates")
				require.NoError(t, os.WriteFile(infoAlternatesPath, []byte(alternatePath), perm.PrivateFile))
			},
			expectedInfo: RepositoryInfo{
				Alternates: []string{
					alternatePath,
				},
			},
		},
		{
			desc: "non-split commit-graph without bloom filter and generation data",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "commitGraph.generationVersion=1",
					"commit-graph", "write", "--reachable",
				)
			},
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 2,
					Size:  hashDependentSize(142, 158),
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
				CommitGraph: CommitGraphInfo{
					Exists: true,
				},
			},
		},
		{
			desc: "non-split commit-graph with bloom filter and no generation data",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c", "commitGraph.generationVersion=1",
					"commit-graph", "write", "--reachable", "--changed-paths",
				)
			},
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 2,
					Size:  hashDependentSize(142, 158),
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
				CommitGraph: CommitGraphInfo{
					Exists:          true,
					HasBloomFilters: true,
				},
			},
		},
		{
			desc: "non-split commit-graph with bloom filters and generation data",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath,
					"-c",
					"commitGraph.generationVersion=2",
					"commit-graph",
					"write",
					"--reachable",
					"--changed-paths",
				)
			},
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 2,
					Size:  hashDependentSize(142, 158),
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
				CommitGraph: CommitGraphInfo{
					Exists:            true,
					HasBloomFilters:   true,
					HasGenerationData: true,
				},
			},
		},
		{
			desc: "all together",
			setup: func(t *testing.T, repoPath string) {
				infoAlternatesPath := filepath.Join(repoPath, "objects", "info", "alternates")
				require.NoError(t, os.WriteFile(infoAlternatesPath, []byte(alternatePath), perm.PrivateFile))

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
					require.NoError(t, os.WriteFile(garbagePath, []byte("x"), perm.PrivateFile))
				}
			},
			expectedInfo: RepositoryInfo{
				LooseObjects: LooseObjectsInfo{
					Count: 2,
					Size:  32,
				},
				Packfiles: PackfilesInfo{
					Count:        1,
					Size:         hashDependentSize(42, 54),
					GarbageCount: 3,
					GarbageSize:  3,
					Bitmap: BitmapInfo{
						Exists:       true,
						Version:      1,
						HasHashCache: true,
					},
				},
				References: ReferencesInfo{
					LooseReferencesCount: 1,
				},
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

			repoInfo, err := RepositoryInfoForRepository(repo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedInfo, repoInfo)
		})
	}
}

func TestReferencesInfoForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc         string
		setup        func(*testing.T, *localrepo.Repo, string)
		expectedInfo ReferencesInfo
	}{
		{
			desc: "empty repository",
			setup: func(*testing.T, *localrepo.Repo, string) {
			},
		},
		{
			desc: "single unpacked reference",
			setup: func(t *testing.T, _ *localrepo.Repo, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			},
			expectedInfo: ReferencesInfo{
				LooseReferencesCount: 1,
			},
		},
		{
			desc: "packed reference",
			setup: func(t *testing.T, _ *localrepo.Repo, repoPath string) {
				// We just write some random garbage -- we don't verify contents
				// anyway, but just the size. And testing like that is at least
				// deterministic as we don't have to special-case hash sizes.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "packed-refs"), []byte("content"), perm.SharedFile))
			},
			expectedInfo: ReferencesInfo{
				PackedReferencesSize: 7,
			},
		},
		{
			desc: "multiple unpacked and packed refs",
			setup: func(t *testing.T, _ *localrepo.Repo, repoPath string) {
				for _, ref := range []string{
					"refs/heads/main",
					"refs/something",
					"refs/merge-requests/1/HEAD",
				} {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference(ref))
				}

				// We just write some random garbage -- we don't verify contents
				// anyway, but just the size. And testing like that is at least
				// deterministic as we don't have to special-case hash sizes.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "packed-refs"), []byte("content"), perm.SharedFile))
			},
			expectedInfo: ReferencesInfo{
				LooseReferencesCount: 3,
				PackedReferencesSize: 7,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			tc.setup(t, repo, repoPath)

			info, err := ReferencesInfoForRepository(repo)
			require.NoError(t, err)
			require.Equal(t, tc.expectedInfo, info)
		})
	}
}

func TestCountLooseObjects(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	createRepo := func(t *testing.T) (*localrepo.Repo, string) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return localrepo.NewTestRepo(t, cfg, repoProto), repoPath
	}

	requireLooseObjectsInfo := func(t *testing.T, repo *localrepo.Repo, cutoff time.Time, expectedInfo LooseObjectsInfo) {
		info, err := LooseObjectsInfoForRepository(repo, cutoff)
		require.NoError(t, err)
		require.Equal(t, expectedInfo, info)
	}

	t.Run("empty repository", func(t *testing.T) {
		repo, _ := createRepo(t)
		requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{})
	})

	t.Run("object in random shard", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		differentShard := filepath.Join(repoPath, "objects", "a0")
		require.NoError(t, os.MkdirAll(differentShard, perm.SharedDir))
		require.NoError(t, os.WriteFile(filepath.Join(differentShard, "123456"), []byte("foobar"), perm.SharedFile))

		requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{
			Count:      1,
			Size:       6,
			StaleCount: 1,
			StaleSize:  6,
		})
	})

	t.Run("objects in multiple shards", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		for i, shard := range []string{"00", "17", "32", "ff"} {
			shardPath := filepath.Join(repoPath, "objects", shard)
			require.NoError(t, os.MkdirAll(shardPath, perm.SharedDir))
			require.NoError(t, os.WriteFile(filepath.Join(shardPath, "123456"), make([]byte, i), perm.SharedFile))
		}

		requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{
			Count:      4,
			Size:       6,
			StaleCount: 4,
			StaleSize:  6,
		})
	})

	t.Run("object in shard with grace period", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		shard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(shard, perm.SharedDir))

		objectPaths := []string{
			filepath.Join(shard, "123456"),
			filepath.Join(shard, "654321"),
		}

		cutoffDate := time.Now()
		afterCutoffDate := cutoffDate.Add(1 * time.Minute)
		beforeCutoffDate := cutoffDate.Add(-1 * time.Minute)

		for _, objectPath := range objectPaths {
			require.NoError(t, os.WriteFile(objectPath, []byte("1"), perm.SharedFile))
			require.NoError(t, os.Chtimes(objectPath, afterCutoffDate, afterCutoffDate))
		}

		// Objects are recent, so with the cutoff-date they shouldn't be counted.
		requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{
			Count: 2,
			Size:  2,
		})

		for i, objectPath := range objectPaths {
			// Modify the object's mtime should cause it to be counted.
			require.NoError(t, os.Chtimes(objectPath, beforeCutoffDate, beforeCutoffDate))

			requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{
				Count:      2,
				Size:       2,
				StaleCount: uint64(i) + 1,
				StaleSize:  uint64(i) + 1,
			})
		}
	})

	t.Run("shard with garbage", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		shard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(shard, perm.SharedDir))

		require.NoError(t, os.WriteFile(filepath.Join(shard, "012345"), []byte("valid"), perm.SharedFile))
		require.NoError(t, os.WriteFile(filepath.Join(shard, "garbage"), []byte("garbage"), perm.SharedFile))

		requireLooseObjectsInfo(t, repo, time.Now(), LooseObjectsInfo{
			Count:        1,
			Size:         5,
			StaleCount:   1,
			StaleSize:    5,
			GarbageCount: 1,
			GarbageSize:  7,
		})
	})
}

func BenchmarkCountLooseObjects(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg := testcfg.Build(b)

	createRepo := func(b *testing.B) (*localrepo.Repo, string) {
		repoProto, repoPath := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return localrepo.NewTestRepo(b, cfg, repoProto), repoPath
	}

	b.Run("empty repository", func(b *testing.B) {
		repo, _ := createRepo(b)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := LooseObjectsInfoForRepository(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with single object", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		objectPath := filepath.Join(repoPath, "objects", "17", "12345")
		require.NoError(b, os.Mkdir(filepath.Dir(objectPath), perm.SharedDir))
		require.NoError(b, os.WriteFile(objectPath, nil, perm.SharedFile))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := LooseObjectsInfoForRepository(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with single object in each shard", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		for i := 0; i < 256; i++ {
			objectPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i), "12345")
			require.NoError(b, os.Mkdir(filepath.Dir(objectPath), perm.SharedDir))
			require.NoError(b, os.WriteFile(objectPath, nil, perm.SharedFile))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := LooseObjectsInfoForRepository(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository hitting loose object limit", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		// Usually we shouldn't have a lot more than `looseObjectCount` objects in the
		// repository because we'd repack as soon as we hit that limit. So this benchmark
		// case tries to estimate the usual upper limit for loose objects we'd typically
		// have.
		//
		// Note that we should ideally just use `housekeeping.looseObjectsLimit` here to
		// derive that value. But due to a cyclic dependency that's not possible, so we
		// just use a hard-coded value instead.
		looseObjectCount := 5

		for i := 0; i < 256; i++ {
			shardPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i))
			require.NoError(b, os.Mkdir(shardPath, perm.SharedDir))

			for j := 0; j < looseObjectCount; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, perm.SharedFile))
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := LooseObjectsInfoForRepository(repo, time.Now())
			require.NoError(b, err)
		}
	})

	b.Run("repository with lots of objects", func(b *testing.B) {
		repo, repoPath := createRepo(b)

		for i := 0; i < 256; i++ {
			shardPath := filepath.Join(repoPath, "objects", fmt.Sprintf("%02x", i))
			require.NoError(b, os.Mkdir(shardPath, perm.SharedDir))

			for j := 0; j < 1000; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, perm.SharedFile))
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := LooseObjectsInfoForRepository(repo, time.Now())
			require.NoError(b, err)
		}
	})
}

func TestPackfileInfoForRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		seedRepository func(t *testing.T, repoPath string)
		expectedInfo   PackfilesInfo
	}{
		{
			desc: "empty repository",
			seedRepository: func(t *testing.T, repoPath string) {
			},
			expectedInfo: PackfilesInfo{},
		},
		{
			desc: "single packfile",
			seedRepository: func(t *testing.T, repoPath string) {
				packfileDir := filepath.Join(repoPath, "objects", "pack")
				require.NoError(t, os.MkdirAll(packfileDir, perm.SharedDir))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				Count: 1,
				Size:  6,
			},
		},
		{
			desc: "keep packfile",
			seedRepository: func(t *testing.T, repoPath string) {
				packfileDir := filepath.Join(repoPath, "objects", "pack")
				require.NoError(t, os.MkdirAll(packfileDir, perm.SharedDir))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), perm.SharedFile))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.keep"), []byte("foobar"), perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				KeepCount: 1,
				KeepSize:  6,
			},
		},
		{
			desc: "cruft packfile",
			seedRepository: func(t *testing.T, repoPath string) {
				packfileDir := filepath.Join(repoPath, "objects", "pack")
				require.NoError(t, os.MkdirAll(packfileDir, perm.SharedDir))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), perm.SharedFile))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.mtimes"), []byte("foobar"), perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				CruftCount: 1,
				CruftSize:  6,
			},
		},
		{
			desc: "multiple packfiles",
			seedRepository: func(t *testing.T, repoPath string) {
				packfileDir := filepath.Join(repoPath, "objects", "pack")
				require.NoError(t, os.MkdirAll(packfileDir, perm.SharedDir))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), perm.SharedFile))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-bar.pack"), []byte("123"), perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				Count: 2,
				Size:  9,
			},
		},
		{
			desc: "reverse index",
			seedRepository: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-c", "pack.writeReverseIndex=true", "-C", repoPath, "repack", "-Ad")
			},
			expectedInfo: PackfilesInfo{
				Count:             1,
				Size:              hashDependentSize(163, 189),
				ReverseIndexCount: 1,
				Bitmap: BitmapInfo{
					Exists:       true,
					Version:      1,
					HasHashCache: true,
				},
			},
		},
		{
			desc: "multi-pack-index",
			seedRepository: func(t *testing.T, repoPath string) {
				packfileDir := filepath.Join(repoPath, "objects", "pack")
				require.NoError(t, os.MkdirAll(packfileDir, perm.SharedDir))
				require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "multi-pack-index"), nil, perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				HasMultiPackIndex: true,
			},
		},
		{
			desc: "multi-pack-index with bitmap",
			seedRepository: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Adb", "--write-midx")
			},
			expectedInfo: PackfilesInfo{
				Count:             1,
				Size:              hashDependentSize(163, 189),
				HasMultiPackIndex: true,
				MultiPackIndexBitmap: BitmapInfo{
					Exists:       true,
					Version:      1,
					HasHashCache: true,
				},
			},
		},
		{
			desc: "multiple packfiles with other data structures",
			seedRepository: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
				gittest.Exec(t, cfg, "-c", "repack.writeBitmaps=false", "-C", repoPath, "repack", "-Ad")
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("second"), gittest.WithBranch("second"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-db", "--write-midx")

				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "pack", "garbage"), []byte("1"), perm.SharedFile))
			},
			expectedInfo: PackfilesInfo{
				Count:             2,
				Size:              hashDependentSize(315, 367),
				GarbageCount:      1,
				GarbageSize:       1,
				HasMultiPackIndex: true,
				MultiPackIndexBitmap: BitmapInfo{
					Exists:       true,
					Version:      1,
					HasHashCache: true,
				},
			},
		},
		{
			desc: "cruft packfiles with other data structures",
			seedRepository: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("first"), gittest.WithBranch("first"))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreachable"))
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "--cruft", "-db", "--write-midx")
			},
			expectedInfo: PackfilesInfo{
				Count:             1,
				Size:              hashDependentSize(162, 188),
				CruftCount:        1,
				CruftSize:         hashDependentSize(156, 183),
				HasMultiPackIndex: true,
				MultiPackIndexBitmap: BitmapInfo{
					Exists:       true,
					Version:      1,
					HasHashCache: true,
				},
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.seedRepository(t, repoPath)

			info, err := PackfilesInfoForRepository(repo)
			require.NoError(t, err)
			require.Equal(t, tc.expectedInfo, info)
		})
	}
}

type packfileEntry struct {
	fs.DirEntry
	name string
}

func (e packfileEntry) Name() string {
	return e.name
}

func TestClassifyPackfiles(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc            string
		packfileEntries []fs.DirEntry
		expectedTypes   map[string]packfileMetadata
	}{
		{
			desc:          "empty entries",
			expectedTypes: map[string]packfileMetadata{},
		},
		{
			desc: "unrelated entry",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "something something"},
			},
			expectedTypes: map[string]packfileMetadata{},
		},
		{
			desc: "packfile index only",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.idx"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {},
			},
		},
		{
			desc: "normal packfile",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.pack"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {},
			},
		},
		{
			desc: "normal packfile with unrelated metadata",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.pack"},
				packfileEntry{name: "pack-5678.keep"},
				packfileEntry{name: "pack-5678.mtimes"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {},
				"pack-5678.pack": {
					hasKeep:   true,
					hasMtimes: true,
				},
			},
		},
		{
			desc: "keep packfile",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.pack"},
				packfileEntry{name: "pack-1234.keep"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {
					hasKeep: true,
				},
			},
		},
		{
			desc: "cruft packfile",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.pack"},
				packfileEntry{name: "pack-1234.mtimes"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {
					hasMtimes: true,
				},
			},
		},
		{
			desc: "keep packfile with mtimes",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1234.pack"},
				packfileEntry{name: "pack-1234.keep"},
				packfileEntry{name: "pack-1234.mtimes"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1234.pack": {
					hasKeep:   true,
					hasMtimes: true,
				},
			},
		},
		{
			desc: "multiple packfiles",
			packfileEntries: []fs.DirEntry{
				packfileEntry{name: "pack-1.pack"},
				packfileEntry{name: "pack-1.keep"},
				packfileEntry{name: "pack-2.pack"},
				packfileEntry{name: "pack-2.mtimes"},
				packfileEntry{name: "pack-3.pack"},
				packfileEntry{name: "pack-3.idx"},
				packfileEntry{name: "pack-4.idx"},
				packfileEntry{name: "garbage"},
			},
			expectedTypes: map[string]packfileMetadata{
				"pack-1.pack": {
					hasKeep: true,
				},
				"pack-2.pack": {
					hasMtimes: true,
				},
				"pack-3.pack": {},
				"pack-4.pack": {},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expectedTypes, classifyPackfiles(tc.packfileEntries))
		})
	}
}

func TestBitmapInfoForPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, bitmapTypeTC := range []struct {
		desc             string
		repackArgs       []string
		verifyBitmapName func(*testing.T, string)
	}{
		{
			desc:       "packfile bitmap",
			repackArgs: []string{"-Adb"},
			verifyBitmapName: func(t *testing.T, bitmapName string) {
				require.Regexp(t, "^pack-.*.bitmap$", bitmapName)
			},
		},
		{
			desc:       "multi-pack-index bitmap",
			repackArgs: []string{"-Adb", "--write-midx"},
			verifyBitmapName: func(t *testing.T, bitmapName string) {
				require.Regexp(t, "^multi-pack-index-.*.bitmap$", bitmapName)
			},
		},
	} {
		bitmapTypeTC := bitmapTypeTC

		t.Run(bitmapTypeTC.desc, func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				desc               string
				writeHashCache     bool
				writeLookupTable   bool
				expectedBitmapInfo BitmapInfo
				expectedErr        error
			}{
				{
					desc:             "bitmap without any extension",
					writeHashCache:   false,
					writeLookupTable: false,
					expectedBitmapInfo: BitmapInfo{
						Exists:  true,
						Version: 1,
					},
				},
				{
					desc:             "bitmap with hash cache",
					writeHashCache:   true,
					writeLookupTable: false,
					expectedBitmapInfo: BitmapInfo{
						Exists:       true,
						Version:      1,
						HasHashCache: true,
					},
				},
				{
					desc:             "bitmap with lookup table",
					writeHashCache:   false,
					writeLookupTable: true,
					expectedBitmapInfo: BitmapInfo{
						Exists:         true,
						Version:        1,
						HasLookupTable: true,
					},
				},
				{
					desc:             "bitmap with all extensions",
					writeHashCache:   true,
					writeLookupTable: true,
					expectedBitmapInfo: BitmapInfo{
						Exists:         true,
						Version:        1,
						HasHashCache:   true,
						HasLookupTable: true,
					},
				},
			} {
				tc := tc

				t.Run(tc.desc, func(t *testing.T) {
					t.Parallel()

					_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
						SkipCreationViaService: true,
					})
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

					gittest.Exec(t, cfg, append([]string{
						"-C", repoPath,
						"-c", "pack.writeBitmapHashCache=" + strconv.FormatBool(tc.writeHashCache),
						"-c", "pack.writeBitmapLookupTable=" + strconv.FormatBool(tc.writeLookupTable),
						"repack",
					}, bitmapTypeTC.repackArgs...)...)

					bitmapPaths, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.bitmap"))
					require.NoError(t, err)
					require.Len(t, bitmapPaths, 1)

					bitmapPath := bitmapPaths[0]
					bitmapTypeTC.verifyBitmapName(t, filepath.Base(bitmapPath))

					bitmapInfo, err := BitmapInfoForPath(bitmapPath)
					require.Equal(t, tc.expectedErr, err)
					require.Equal(t, tc.expectedBitmapInfo, bitmapInfo)
				})
			}
		})
	}

	for _, tc := range []struct {
		desc        string
		setup       func(t *testing.T) string
		expectedErr error
	}{
		{
			desc: "nonexistent path",
			setup: func(t *testing.T) string {
				return "/does/not/exist"
			},
			expectedErr: fmt.Errorf("opening bitmap: %w", &fs.PathError{
				Op:   "open",
				Path: "/does/not/exist",
				Err:  syscall.ENOENT,
			}),
		},
		{
			desc: "header is too short",
			setup: func(t *testing.T) string {
				bitmapPath := filepath.Join(testhelper.TempDir(t), "bitmap")
				require.NoError(t, os.WriteFile(bitmapPath, []byte{0, 0, 0}, perm.SharedFile))
				return bitmapPath
			},
			expectedErr: fmt.Errorf("reading bitmap header: %w", io.ErrUnexpectedEOF),
		},
		{
			desc: "invalid signature",
			setup: func(t *testing.T) string {
				bitmapPath := filepath.Join(testhelper.TempDir(t), "bitmap")
				require.NoError(t, os.WriteFile(bitmapPath, []byte{
					'B', 'I', 'T', 'O', 0, 0, 0, 0,
				}, perm.SharedFile))
				return bitmapPath
			},
			expectedErr: fmt.Errorf("invalid bitmap signature: %q", "BITO"),
		},
		{
			desc: "unsupported version",
			setup: func(t *testing.T) string {
				bitmapPath := filepath.Join(testhelper.TempDir(t), "bitmap")
				require.NoError(t, os.WriteFile(bitmapPath, []byte{
					'B', 'I', 'T', 'M', 0, 2, 0, 0,
				}, perm.SharedFile))
				return bitmapPath
			},
			expectedErr: fmt.Errorf("unsupported version: 2"),
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			bitmapPath := tc.setup(t)

			bitmapInfo, err := BitmapInfoForPath(bitmapPath)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, BitmapInfo{}, bitmapInfo)
		})
	}
}

func hashDependentSize(sha1, sha256 uint64) uint64 {
	if gittest.DefaultObjectHash.Format == "sha1" {
		return sha1
	}
	return sha256
}
