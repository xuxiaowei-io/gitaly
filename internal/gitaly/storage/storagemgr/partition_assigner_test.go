package storagemgr

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestPartitionAssigner(t *testing.T) {
	db, err := OpenDatabase(testhelper.NewDiscardingLogger(t), t.TempDir())
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	pa, err := newPartitionAssigner(db)
	require.NoError(t, err)
	defer testhelper.MustClose(t, pa)

	ctx := testhelper.Context(t)

	relativePath1 := "relative-path-1"
	// The relative path should get assigned into partition.
	ptnID1, err := pa.getPartitionID(ctx, relativePath1)
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID1, "first allocated partition id should be 1")

	// The second repository should land into its own partition.
	ptnID2, err := pa.getPartitionID(ctx, "relative-path-2")
	require.NoError(t, err)
	require.EqualValues(t, 2, ptnID2)

	// Retrieving the first repository's partition should return the partition that
	// was assigned earlier.
	ptnID1, err = pa.getPartitionID(ctx, relativePath1)
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID1)
}

func TestPartitionAssigner_close(t *testing.T) {
	dbDir := t.TempDir()

	db, err := OpenDatabase(testhelper.NewDiscardingLogger(t), dbDir)
	require.NoError(t, err)

	pa, err := newPartitionAssigner(db)
	require.NoError(t, err)
	testhelper.MustClose(t, pa)
	testhelper.MustClose(t, db)

	db, err = OpenDatabase(testhelper.NewDiscardingLogger(t), dbDir)
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	pa, err = newPartitionAssigner(db)
	require.NoError(t, err)
	defer testhelper.MustClose(t, pa)

	// A block of ID is loaded into memory when the partitionAssigner is initialized.
	// Closing the partitionAssigner is expected to return the unused IDs in the block
	// back to the database.
	ptnID, err := pa.getPartitionID(testhelper.Context(t), "relative-path")
	require.NoError(t, err)
	require.EqualValues(t, 1, ptnID)
}

func TestPartitionAssigner_concurrentAccess(t *testing.T) {
	db, err := OpenDatabase(testhelper.NewDiscardingLogger(t), t.TempDir())
	require.NoError(t, err)
	defer testhelper.MustClose(t, db)

	pa, err := newPartitionAssigner(db)
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
	for i := 0; i < repositoryCount; i++ {
		i := i
		collectedIDs[i] = make([]partitionID, goroutineCount)
		relativePath := fmt.Sprintf("relative-path-%d", i)
		for j := 0; j < goroutineCount; j++ {
			j := j
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ptnID, err := pa.getPartitionID(ctx, relativePath)
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

	// We expect to have 10 unique partition IDs as there are 10 repositories being accessed.
	require.ElementsMatch(t, []partitionID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, partitionIDs)
}
