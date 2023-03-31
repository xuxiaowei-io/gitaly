package gitaly

import (
	"context"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	repo "gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
)

// ErrPartitionManagerStopped is returned when the PartitionManager stops processing transactions.
var ErrPartitionManagerStopped = errors.New("partition manager stopped")

// PartitionManager is responsible for managing the lifecycle of each TransactionManager.
type PartitionManager struct {
	// mu is the mutex to synchronize access to the partitions.
	mu sync.Mutex
	// db is the handle to the key-value store used for storing the write-ahead log related state.
	// It is used to create each transaction manager.
	db *badger.DB
	// partitions contains all the active partitions for which there are pending transactions.
	// Each repository can have up to one partition.
	partitions map[string]*partition
	// localRepoFactory is used by PartitionManager to construct `localrepo.Repo`.
	localRepoFactory func(repo.GitRepo) *localrepo.Repo
	// logger handles all logging for PartitionManager.
	logger logrus.FieldLogger
	// stopped tracks whether the PartitionManager has been stopped. If the manager is stopped,
	// no new transactions are allowed to begin.
	stopped bool
	// partitionsWG keeps track of running partitions.
	partitionsWG sync.WaitGroup
}

// partition contains the transaction manager and tracks the number of in-flight transactions for the partition.
type partition struct {
	// shuttingDown is set when the partition shutdown was initiated due to being idle.
	shuttingDown bool
	// shutdown is closed to signal when the partition is finished shutting down. Clients stumbling on the
	// partition when it is shutting down wait on this channel to know when the partition has shut down and they
	// should retry.
	shutdown chan struct{}
	// transactionManager manages all transactions for the partition.
	transactionManager *TransactionManager
	// pendingTransactionCount holds the current number of in flight transactions being processed by the manager.
	pendingTransactionCount uint
}

// NewPartitionManager returns a new PartitionManager.
func NewPartitionManager(db *badger.DB, localRepoFactory func(repo.GitRepo) *localrepo.Repo, logger logrus.FieldLogger) *PartitionManager {
	return &PartitionManager{
		db:               db,
		partitions:       make(map[string]*partition),
		localRepoFactory: localRepoFactory,
		logger:           logger,
	}
}

// Begin gets the TransactionManager for the specified repository and starts a Transaction. If a
// TransactionManager is not already running, a new one is created and used. The partition tracks
// the number of pending transactions and this counter gets incremented when Begin is invoked.
func (pm *PartitionManager) Begin(ctx context.Context, repo repo.GitRepo) (*Transaction, error) {
	localRepo := pm.localRepoFactory(repo)
	partitionKey := getRepositoryID(localRepo)

	for {
		pm.mu.Lock()
		if pm.stopped {
			pm.mu.Unlock()
			return nil, ErrPartitionManagerStopped
		}

		ptn, ok := pm.partitions[partitionKey]
		if !ok {
			ptn = &partition{
				shutdown: make(chan struct{}),
			}

			mgr := NewTransactionManager(pm.db, localRepo, pm.transactionFinalizerFactory(ptn))
			ptn.transactionManager = mgr

			pm.partitions[partitionKey] = ptn

			pm.partitionsWG.Add(1)
			go func() {
				if err := mgr.Run(); err != nil {
					pm.logger.WithError(err).Error("partition failed")
				}

				// In the event that TransactionManager stops running, a new TransactionManager will
				// need to be started in order to continue processing transactions. The partition is
				// deleted allowing the next transaction for the repository to create a new partition
				// and TransactionManager.
				pm.mu.Lock()
				delete(pm.partitions, partitionKey)
				pm.mu.Unlock()

				close(ptn.shutdown)
				pm.partitionsWG.Done()
			}()
		}

		if ptn.shuttingDown {
			// If the partition is in the process of shutting down, the partition should not be
			// used. The lock is released while waiting for the partition to complete shutdown as to
			// not block other partitions from processing transactions. Once shutdown is complete, a
			// new attempt is made to get a valid partition.
			pm.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ptn.shutdown:
			}

			continue
		}

		ptn.pendingTransactionCount++
		pm.mu.Unlock()

		transaction, err := ptn.transactionManager.Begin(ctx)
		if err != nil {
			// The pending transaction count needs to be decremented since the transaction is no longer
			// inflight. A transaction failing does not necessarily mean the transaction manager has
			// stopped running. Consequently, if there are no other pending transactions the partition
			// should be stopped.
			pm.transactionFinalizerFactory(ptn)()

			return nil, err
		}

		return transaction, nil
	}
}

// Stop stops transaction processing for all running transaction managers and waits for shutdown
// completion.
func (pm *PartitionManager) Stop() {
	pm.mu.Lock()
	// Mark the PartitionManager as stopped so no new transactions can begin anymore. This
	// also means no more partitions are spawned.
	pm.stopped = true
	for _, ptn := range pm.partitions {
		// Stop all partitions.
		ptn.stop()
	}
	pm.mu.Unlock()

	// Wait for all goroutines to complete.
	pm.partitionsWG.Wait()
}

// stop stops the partition's transaction manager.
func (ptn *partition) stop() {
	ptn.shuttingDown = true
	ptn.transactionManager.Stop()
}

// transactionFinalizerFactory is executed when a transaction completes. The pending transaction counter
// for the partition is decremented by one and TransactionManager stopped if there are no longer
// any pending transactions.
func (pm *PartitionManager) transactionFinalizerFactory(ptn *partition) func() {
	return func() {
		pm.mu.Lock()
		defer pm.mu.Unlock()

		ptn.pendingTransactionCount--
		if ptn.pendingTransactionCount == 0 {
			ptn.stop()
		}
	}
}
