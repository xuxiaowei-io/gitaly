package stats

import (
	"fmt"
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
					Count:     1,
					Size:      hashDependentSize(42, 54),
					HasBitmap: true,
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
					Count:     1,
					Size:      hashDependentSize(42, 54),
					HasBitmap: true,
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
				require.NoError(t, os.WriteFile(garbagePath, []byte("x"), 0o600))
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
				require.NoError(t, os.WriteFile(infoAlternatesPath, []byte(alternatePath), 0o600))
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
					HasBitmap:    true,
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
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "packed-refs"), []byte("content"), 0o644))
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
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "packed-refs"), []byte("content"), 0o644))
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
		require.NoError(t, os.MkdirAll(differentShard, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(differentShard, "123456"), []byte("foobar"), 0o644))

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
			require.NoError(t, os.MkdirAll(shardPath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(shardPath, "123456"), make([]byte, i), 0o644))
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
		require.NoError(t, os.MkdirAll(shard, 0o755))

		objectPaths := []string{
			filepath.Join(shard, "123456"),
			filepath.Join(shard, "654321"),
		}

		cutoffDate := time.Now()
		afterCutoffDate := cutoffDate.Add(1 * time.Minute)
		beforeCutoffDate := cutoffDate.Add(-1 * time.Minute)

		for _, objectPath := range objectPaths {
			require.NoError(t, os.WriteFile(objectPath, []byte("1"), 0o644))
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
		require.NoError(t, os.MkdirAll(shard, 0o755))

		require.NoError(t, os.WriteFile(filepath.Join(shard, "012345"), []byte("valid"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(shard, "garbage"), []byte("garbage"), 0o644))

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
		require.NoError(b, os.Mkdir(filepath.Dir(objectPath), 0o755))
		require.NoError(b, os.WriteFile(objectPath, nil, 0o644))

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
			require.NoError(b, os.Mkdir(filepath.Dir(objectPath), 0o755))
			require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
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
			require.NoError(b, os.Mkdir(shardPath, 0o755))

			for j := 0; j < looseObjectCount; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
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
			require.NoError(b, os.Mkdir(shardPath, 0o755))

			for j := 0; j < 1000; j++ {
				objectPath := filepath.Join(shardPath, fmt.Sprintf("%d", j))
				require.NoError(b, os.WriteFile(objectPath, nil, 0o644))
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

	createRepo := func(t *testing.T) (*localrepo.Repo, string) {
		repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		return localrepo.NewTestRepo(t, cfg, repoProto), repoPath
	}

	requirePackfilesInfo := func(t *testing.T, repo *localrepo.Repo, expectedInfo PackfilesInfo) {
		info, err := PackfilesInfoForRepository(repo)
		require.NoError(t, err)
		require.Equal(t, expectedInfo, info)
	}

	t.Run("empty repository", func(t *testing.T) {
		repo, _ := createRepo(t)
		requirePackfilesInfo(t, repo, PackfilesInfo{})
	})

	t.Run("single packfile", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		packfileDir := filepath.Join(repoPath, "objects", "pack")
		require.NoError(t, os.MkdirAll(packfileDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), 0o644))

		requirePackfilesInfo(t, repo, PackfilesInfo{
			Count: 1,
			Size:  6,
		})
	})

	t.Run("multiple packfiles", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		packfileDir := filepath.Join(repoPath, "objects", "pack")
		require.NoError(t, os.MkdirAll(packfileDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-foo.pack"), []byte("foobar"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "pack-bar.pack"), []byte("123"), 0o644))

		requirePackfilesInfo(t, repo, PackfilesInfo{
			Count: 2,
			Size:  9,
		})
	})

	t.Run("multi-pack-index", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		packfileDir := filepath.Join(repoPath, "objects", "pack")
		require.NoError(t, os.MkdirAll(packfileDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "multi-pack-index"), nil, 0o644))

		requirePackfilesInfo(t, repo, PackfilesInfo{
			HasMultiPackIndex: true,
		})
	})

	t.Run("multi-pack-index with bitmap", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		packfileDir := filepath.Join(repoPath, "objects", "pack")
		require.NoError(t, os.MkdirAll(packfileDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "multi-pack-index"), nil, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(packfileDir, "multi-pack-index-c0363841cc7e5783a996c72f0a4a7ae4440aaa40.bitmap"), nil, 0o644))

		requirePackfilesInfo(t, repo, PackfilesInfo{
			HasMultiPackIndex:       true,
			HasMultiPackIndexBitmap: true,
		})
	})

	t.Run("multiple packfiles with other data structures", func(t *testing.T) {
		repo, repoPath := createRepo(t)

		packfileDir := filepath.Join(repoPath, "objects", "pack")
		require.NoError(t, os.MkdirAll(packfileDir, 0o755))
		for _, file := range []string{
			"pack-bar.bar",
			"pack-bar.pack",
			"pack-bar.idx",
			"pack-foo.bar",
			"pack-foo.pack",
			"pack-foo.idx",
			"garbage",
			"multi-pack-index",
			"multi-pack-index-c0363841cc7e5783a996c72f0a4a7ae4440aaa40.bitmap",
		} {
			require.NoError(t, os.WriteFile(filepath.Join(packfileDir, file), []byte("1"), 0o644))
		}

		requirePackfilesInfo(t, repo, PackfilesInfo{
			Count:                   2,
			Size:                    2,
			GarbageCount:            1,
			GarbageSize:             1,
			HasMultiPackIndex:       true,
			HasMultiPackIndexBitmap: true,
		})
	})
}

func hashDependentSize(sha1, sha256 uint64) uint64 {
	if gittest.DefaultObjectHash.Format == "sha1" {
		return sha1
	}
	return sha256
}
