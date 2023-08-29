package storagemgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// errPartitionAssignmentNotFound is returned when attempting to access a
// partition assignment in the database that doesn't yet exist.
var errPartitionAssignmentNotFound = errors.New("partition assignment not found")

// partitionID uniquely identifies a partition.
type partitionID uint64

func (id partitionID) MarshalBinary() []byte {
	marshaled := make([]byte, binary.Size(id))
	binary.BigEndian.PutUint64(marshaled, uint64(id))
	return marshaled
}

func (id *partitionID) UnmarshalBinary(data []byte) {
	*id = partitionID(binary.BigEndian.Uint64(data))
}

func (id partitionID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

// partitionAssignmentTable records which partitions repositories are assigned into.
type partitionAssignmentTable struct{ db *badger.DB }

func newPartitionAssignmentTable(db *badger.DB) *partitionAssignmentTable {
	return &partitionAssignmentTable{db: db}
}

func (pt *partitionAssignmentTable) key(relativePath string) []byte {
	return []byte(fmt.Sprintf("partition_assignment/%s", relativePath))
}

func (pt *partitionAssignmentTable) getPartitionID(relativePath string) (partitionID, error) {
	var id partitionID
	if err := pt.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(pt.key(relativePath))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return errPartitionAssignmentNotFound
			}

			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error {
			id.UnmarshalBinary(value)
			return nil
		})
	}); err != nil {
		return 0, fmt.Errorf("view: %w", err)
	}

	return id, nil
}

func (pt *partitionAssignmentTable) setPartitionID(relativePath string, id partitionID) error {
	wb := pt.db.NewWriteBatch()
	if err := wb.Set(pt.key(relativePath), id.MarshalBinary()); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

// partitionAssigner manages assignment of repositories in to partitions.
type partitionAssigner struct {
	// mutex synchronizes access to repositoryLocks.
	mutex sync.Mutex
	// repositoryLocks holds per-repository locks. The key is a relative path and the
	// channel closing signals the lock being released.
	repositoryLocks map[string]chan struct{}
	// idSequence is the sequence used to mint partition IDs.
	idSequence *badger.Sequence
	// partitionAssignmentTable contains the partition assignment records.
	partitionAssignmentTable *partitionAssignmentTable
}

// newPartitionAssigner returns a new partitionAssigner. Close must be called on the
// returned instance to release acquired resources.
func newPartitionAssigner(db *badger.DB) (*partitionAssigner, error) {
	seq, err := db.GetSequence([]byte("partition_id_seq"), 100)
	if err != nil {
		return nil, fmt.Errorf("get sequence: %w", err)
	}

	return &partitionAssigner{
		repositoryLocks:          make(map[string]chan struct{}),
		idSequence:               seq,
		partitionAssignmentTable: newPartitionAssignmentTable(db),
	}, nil
}

func (pa *partitionAssigner) Close() error {
	return pa.idSequence.Release()
}

func (pa *partitionAssigner) allocatePartitionID() (partitionID, error) {
	// Start partition IDs from 1 so the default value refers to an invalid
	// partition.
	var id uint64
	for id == 0 {
		var err error
		id, err = pa.idSequence.Next()
		if err != nil {
			return 0, fmt.Errorf("next: %w", err)
		}
	}

	return partitionID(id), nil
}

// getPartititionID returns the partition ID of the repository. If the repository wasn't yet assigned into
// a partition, it will be assigned into one and the assignment stored. Further accesses return the stored
// partition ID. Each repository goes into its own partition. The method is safe to call concurrently.
func (pa *partitionAssigner) getPartitionID(ctx context.Context, relativePath string) (partitionID, error) {
	ptnID, err := pa.partitionAssignmentTable.getPartitionID(relativePath)
	if err != nil {
		if !errors.Is(err, errPartitionAssignmentNotFound) {
			return 0, fmt.Errorf("get partition: %w", err)
		}

		// Repository wasn't yet assigned into a partition.

		pa.mutex.Lock()
		// See if some other goroutine already locked the repository. If so, wait for it to complete
		// and get the partition ID it set.
		if lock, ok := pa.repositoryLocks[relativePath]; ok {
			pa.mutex.Unlock()
			// Some other goroutine is already assigning a partition for the
			// repository. Wait for it to complete and then get the partition.
			select {
			case <-lock:
				ptnID, err := pa.partitionAssignmentTable.getPartitionID(relativePath)
				if err != nil {
					return 0, fmt.Errorf("get partition ID after waiting: %w", err)
				}
				return ptnID, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		// No other goroutine had locked the repository yet. Lock the repository so other goroutines
		// wait while we assign the repository a partition.
		lock := make(chan struct{})
		pa.repositoryLocks[relativePath] = lock
		pa.mutex.Unlock()
		defer func() {
			close(lock)
			pa.mutex.Lock()
			delete(pa.repositoryLocks, relativePath)
			pa.mutex.Unlock()
		}()

		// With the repository locked, check first whether someone else assigned it into a partition
		// while we weren't holding the lock between the first failed attempt getting the assignment
		// and locking the repository.
		ptnID, err = pa.partitionAssignmentTable.getPartitionID(relativePath)
		if !errors.Is(err, errPartitionAssignmentNotFound) {
			if err != nil {
				return 0, fmt.Errorf("recheck partition: %w", err)
			}

			// Some other goroutine assigned a partition between the failed attempt and locking the
			// repository.
			return ptnID, nil
		}

		// Each repository goes into its own partition. Allocate a new partition ID for this
		// repository.
		ptnID, err = pa.allocatePartitionID()
		if err != nil {
			return 0, fmt.Errorf("acquire partition id: %w", err)
		}

		if err := pa.partitionAssignmentTable.setPartitionID(relativePath, ptnID); err != nil {
			return 0, fmt.Errorf("set partition: %w", err)
		}
	}

	return ptnID, nil
}
