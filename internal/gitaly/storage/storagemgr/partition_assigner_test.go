package storagemgr

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

type partitionAssignments map[string]partitionID

func getPartitionAssignments(tb testing.TB, db Database) partitionAssignments {
	tb.Helper()

	state := partitionAssignments{}
	require.NoError(tb, db.View(func(txn DatabaseTransaction) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix: []byte(prefixPartitionAssignment),
		})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			require.NoError(tb, err)

			var ptnID partitionID
			ptnID.UnmarshalBinary(value)

			relativePath := strings.TrimPrefix(string(it.Item().Key()), prefixPartitionAssignment)
			state[relativePath] = ptnID
		}

		return nil
	}))

	return state
}

func TestPartitionAssigner(t *testing.T) {
	db, err := OpenDatabase(testhelper.SharedLogger(t), t.TempDir())
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	cfg := testcfg.Build(t)
	pa, err := newPartitionAssigner(db, cfg.Storages[0].Path)
	require.NoError(t, err)
	defer testhelper.MustClose(t, pa)

	ctx := testhelper.Context(t)

	relativePath1 := "relative-path-1"
	// The relative path should get assigned into partition.
	ptnID1, err := pa.getPartitionID(ctx, relativePath1, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID1, "first allocated partition id should be 1")

	// The second repository should land into its own partition.
	ptnID2, err := pa.getPartitionID(ctx, "relative-path-2", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, ptnID2)

	// Retrieving the first repository's partition should return the partition that
	// was assigned earlier.
	ptnID1, err = pa.getPartitionID(ctx, relativePath1, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID1)

	// Repository should get assigned into the same partition as the alternate.
	ptnID3, err := pa.getPartitionID(ctx, "relative-path-3", relativePath1)
	require.NoError(t, err)
	require.Equal(t, ptnID3, ptnID1)

	// The alternate should get assigned into the same partition as the target repository
	// if it already has a partition.
	ptnID4, err := pa.getPartitionID(ctx, relativePath1, "relative-path-4")
	require.NoError(t, err)
	require.Equal(t, ptnID4, ptnID1)

	// If neither repository are yet assigned, they should both get assigned into the same
	// partition.
	const relativePath6 = "relative-path-6"
	ptnID5, err := pa.getPartitionID(ctx, "relative-path-5", relativePath6)
	require.NoError(t, err)
	require.EqualValues(t, 3, ptnID5)

	// Getting a partition fails if the repositories are in different partitions.
	ptnID6, err := pa.getPartitionID(ctx, relativePath1, relativePath6)
	require.Equal(t, ErrRepositoriesAreInDifferentPartitions, err)
	require.EqualValues(t, 0, ptnID6)

	require.Equal(t, partitionAssignments{
		relativePath1:     1,
		"relative-path-2": 2,
		"relative-path-3": 1,
		"relative-path-4": 1,
		"relative-path-5": 3,
		relativePath6:     3,
	}, getPartitionAssignments(t, db))
}

func TestPartitionAssigner_alternates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc                         string
		memberAlternatesContent      []byte
		poolAlternatesContent        []byte
		expectedError                error
		expectedPartitionAssignments partitionAssignments
	}{
		{
			desc: "no alternates file",
			expectedPartitionAssignments: partitionAssignments{
				"member": 1,
				"pool":   2,
			},
		},
		{
			desc:                    "empty alternates file",
			memberAlternatesContent: []byte(""),
			expectedPartitionAssignments: partitionAssignments{
				"member": 1,
				"pool":   2,
			},
		},
		{
			desc:                    "not a git directory",
			memberAlternatesContent: []byte("../.."),
			expectedError:           storage.InvalidGitDirectoryError{MissingEntry: "objects"},
		},
		{
			desc:                    "points to pool",
			memberAlternatesContent: []byte("../../pool/objects"),
			expectedPartitionAssignments: partitionAssignments{
				"member": 1,
				"pool":   1,
			},
		},
		{
			desc:                    "points to pool with newline",
			memberAlternatesContent: []byte("../../pool/objects\n"),
			expectedPartitionAssignments: partitionAssignments{
				"member": 1,
				"pool":   1,
			},
		},
		{
			desc:                    "multiple alternates fail",
			memberAlternatesContent: []byte("../../pool/objects\nother-alternate"),
			expectedError:           errMultipleAlternates,
		},
		{
			desc:                    "alternate pointing to self fails",
			memberAlternatesContent: []byte("../objects"),
			expectedError:           errAlternatePointsToSelf,
		},
		{
			desc:                    "alternate having an alternate fails",
			poolAlternatesContent:   []byte("unexpected"),
			memberAlternatesContent: []byte("../../pool/objects"),
			expectedError:           errAlternateHasAlternate,
		},
		{
			desc:                    "alternate points outside the storage",
			memberAlternatesContent: []byte("../../../../.."),
			expectedError:           storage.ErrRelativePathEscapesRoot,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cfg := testcfg.Build(t)

			ctx := testhelper.Context(t)
			poolRepo, poolPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           "pool",
			})

			memberRepo, memberPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
				RelativePath:           "member",
			})

			writeAlternatesFile := func(t *testing.T, repoPath string, content []byte) {
				t.Helper()
				require.NoError(t, os.WriteFile(stats.AlternatesFilePath(repoPath), content, os.ModePerm))
			}

			if tc.poolAlternatesContent != nil {
				writeAlternatesFile(t, poolPath, tc.poolAlternatesContent)
			}

			if tc.memberAlternatesContent != nil {
				writeAlternatesFile(t, memberPath, tc.memberAlternatesContent)
			}

			db, err := OpenDatabase(testhelper.NewLogger(t), t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, db)

			pa, err := newPartitionAssigner(db, cfg.Storages[0].Path)
			require.NoError(t, err)
			defer testhelper.MustClose(t, pa)

			expectedPartitionAssignments := tc.expectedPartitionAssignments
			if expectedPartitionAssignments == nil {
				expectedPartitionAssignments = partitionAssignments{}
			}

			if memberPartitionID, err := pa.getPartitionID(ctx, memberRepo.RelativePath, ""); tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, expectedPartitionAssignments["member"], memberPartitionID)

				poolPartitionID, err := pa.getPartitionID(ctx, poolRepo.RelativePath, "")
				require.NoError(t, err)
				require.Equal(t, expectedPartitionAssignments["pool"], poolPartitionID)
			}

			require.Equal(t, expectedPartitionAssignments, getPartitionAssignments(t, db))
		})
	}
}

func TestPartitionAssigner_close(t *testing.T) {
	dbDir := t.TempDir()

	db, err := OpenDatabase(testhelper.SharedLogger(t), dbDir)
	require.NoError(t, err)

	cfg := testcfg.Build(t)

	pa, err := newPartitionAssigner(db, cfg.Storages[0].Path)
	require.NoError(t, err)
	testhelper.MustClose(t, pa)
	testhelper.MustClose(t, db)

	db, err = OpenDatabase(testhelper.SharedLogger(t), dbDir)
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	pa, err = newPartitionAssigner(db, cfg.Storages[0].Path)
	require.NoError(t, err)
	defer testhelper.MustClose(t, pa)

	// A block of ID is loaded into memory when the partitionAssigner is initialized.
	// Closing the partitionAssigner is expected to return the unused IDs in the block
	// back to the database.
	ptnID, err := pa.getPartitionID(testhelper.Context(t), "relative-path", "")
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID)
}

func TestPartitionAssigner_concurrentAccess(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc          string
		withAlternate bool
	}{
		{
			desc: "without alternate",
		},
		{
			desc:          "with alternate",
			withAlternate: true,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			db, err := OpenDatabase(testhelper.SharedLogger(t), t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, db)

			cfg := testcfg.Build(t)

			pa, err := newPartitionAssigner(db, cfg.Storages[0].Path)
			require.NoError(t, err)
			defer testhelper.MustClose(t, pa)

			// Access 10 repositories concurrently.
			repositoryCount := 10
			// Access each repository from 10 goroutines concurrently.
			goroutineCount := 10

			collectedIDs := make([][]partitionID, repositoryCount)
			ctx := testhelper.Context(t)
			wg := sync.WaitGroup{}
			start := make(chan struct{})

			pool, poolPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			for i := 0; i < repositoryCount; i++ {
				i := i
				collectedIDs[i] = make([]partitionID, goroutineCount)

				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				if tc.withAlternate {
					// Link the repositories to the pool.
					alternateRelativePath, err := filepath.Rel(
						filepath.Join(repoPath, "objects"),
						filepath.Join(poolPath, "objects"),
					)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), []byte(alternateRelativePath), fs.ModePerm))

					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						_, err := pa.getPartitionID(ctx, repo.RelativePath, "")
						assert.NoError(t, err)
					}()
				}

				for j := 0; j < goroutineCount; j++ {
					j := j
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						ptnID, err := pa.getPartitionID(ctx, repo.RelativePath, "")
						assert.NoError(t, err)
						collectedIDs[i][j] = ptnID
					}()
				}
			}

			close(start)
			wg.Wait()

			var partitionIDs []partitionID
			for _, ids := range collectedIDs {
				partitionIDs = append(partitionIDs, ids[0])
				for i := range ids {
					// We expect all goroutines accessing a given repository to get the
					// same partition ID for it.
					require.Equal(t, ids[0], ids[i], ids)
				}
			}

			if tc.withAlternate {
				// We expect all repositories to have been assigned to the same partition as they are all linked to the same pool.
				require.Equal(t, []partitionID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, partitionIDs)
				ptnID, err := pa.getPartitionID(ctx, pool.RelativePath, "")
				require.NoError(t, err)
				require.Equal(t, partitionID(1), ptnID, "pool should have been assigned into the same partition as the linked repositories")
				return
			}

			// We expect to have 10 unique partition IDs as there are 10 repositories being accessed.
			require.ElementsMatch(t, []partitionID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, partitionIDs)
		})
	}
}
