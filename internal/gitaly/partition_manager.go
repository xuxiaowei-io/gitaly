package gitaly

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	repo "gitlab.com/gitlab-org/gitaly/v16/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// ErrPartitionManagerStopped is returned when the PartitionManager stops processing transactions.
var ErrPartitionManagerStopped = errors.New("partition manager stopped")

// PartitionManager is responsible for managing the lifecycle of each TransactionManager.
type PartitionManager struct {
	// storages are the storages configured in this Gitaly server. The map is keyed by the storage name.
	storages map[string]*storageManager
}

// storageManager represents a single storage.
type storageManager struct {
	// mu synchronizes access to the fields of storageManager.
	mu sync.Mutex
	// logger handles all logging for storageManager.
	logger logrus.FieldLogger
	// path is the absolute path to the storage's root.
	path string
	// repoFactory is a factory type that builds localrepo instances for this storage.
	repoFactory localrepo.StorageScopedFactory
	// stagingDirectory is the directory where all of the TransactionManager staging directories
	// should be created.
	stagingDirectory string
	// stopped tracks whether the storageManager has been stopped. If it is stopped,
	// no new transactions are allowed to begin.
	stopped bool
	// db is the handle to the key-value store used for storing the storage's database state.
	database *badger.DB
	// partitions contains all the active partitions. Each repository can have up to one partition.
	partitions map[string]*partition
	// activePartitions keeps track of active partitions.
	activePartitions sync.WaitGroup
}

func (sm *storageManager) stop() {
	sm.mu.Lock()
	// Mark the storage as stopped so no new transactions can begin anymore. This
	// also means no more partitions are spawned.
	sm.stopped = true
	for _, ptn := range sm.partitions {
		// Stop all partitions.
		ptn.stop()
	}
	sm.mu.Unlock()

	// Wait for all partitions to finish.
	sm.activePartitions.Wait()

	if err := sm.database.Close(); err != nil {
		sm.logger.WithError(err).Error("failed closing storage's database")
	}
}

// transactionFinalizerFactory is executed when a transaction completes. The pending transaction counter
// for the partition is decremented by one and TransactionManager stopped if there are no longer
// any pending transactions.
func (sm *storageManager) transactionFinalizerFactory(ptn *partition) func() {
	return func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()

		ptn.pendingTransactionCount--
		if ptn.pendingTransactionCount == 0 {
			ptn.stop()
		}
	}
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

// stop stops the partition's transaction manager.
func (ptn *partition) stop() {
	ptn.shuttingDown = true
	ptn.transactionManager.Stop()
}

// NewPartitionManager returns a new PartitionManager.
func NewPartitionManager(configuredStorages []config.Storage, localRepoFactory localrepo.Factory, logger logrus.FieldLogger) (*PartitionManager, error) {
	storages := make(map[string]*storageManager, len(configuredStorages))
	for _, storage := range configuredStorages {
		repoFactory, err := localRepoFactory.ScopeByStorage(storage.Name)
		if err != nil {
			return nil, fmt.Errorf("scope by storage: %w", err)
		}

		stagingDir := stagingDirectoryPath(storage.Path)
		// Remove a possible already existing staging directory as it may contain stale files
		// if the previous process didn't shutdown gracefully.
		if err := os.RemoveAll(stagingDir); err != nil {
			return nil, fmt.Errorf("failed clearing storage's staging directory: %w", err)
		}

		if err := os.Mkdir(stagingDir, perm.PrivateDir); err != nil {
			return nil, fmt.Errorf("create storage's staging directory: %w", err)
		}

		databaseDir := filepath.Join(storage.Path, "database")
		if err := os.Mkdir(databaseDir, perm.PrivateDir); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("create storage's database directory: %w", err)
		}

		if err := safe.NewSyncer().SyncHierarchy(storage.Path, "database"); err != nil {
			return nil, fmt.Errorf("sync database directory: %w", err)
		}

		db, err := OpenDatabase(databaseDir)
		if err != nil {
			return nil, fmt.Errorf("create storage's database directory: %w", err)
		}

		storages[storage.Name] = &storageManager{
			logger:           logger.WithField("storage", storage.Name),
			path:             storage.Path,
			repoFactory:      repoFactory,
			stagingDirectory: stagingDir,
			database:         db,
			partitions:       map[string]*partition{},
		}
	}

	return &PartitionManager{storages: storages}, nil
}

func stagingDirectoryPath(storagePath string) string {
	return filepath.Join(storagePath, "staging")
}

// getPartitionKey returns a partitions's key.
func getPartitionKey(storageName, relativePath string) string {
	return storageName + ":" + relativePath
}

// Begin gets the TransactionManager for the specified repository and starts a Transaction. If a
// TransactionManager is not already running, a new one is created and used. The partition tracks
// the number of pending transactions and this counter gets incremented when Begin is invoked.
func (pm *PartitionManager) Begin(ctx context.Context, repo repo.GitRepo) (*Transaction, error) {
	storageMgr, ok := pm.storages[repo.GetStorageName()]
	if !ok {
		return nil, structerr.NewNotFound("unknown storage: %q", repo.GetStorageName())
	}

	relativePath, err := storage.ValidateRelativePath(storageMgr.path, repo.GetRelativePath())
	if err != nil {
		return nil, structerr.NewInvalidArgument("validate relative path: %w", err)
	}

	partitionKey := getPartitionKey(repo.GetStorageName(), relativePath)

	for {
		storageMgr.mu.Lock()
		if storageMgr.stopped {
			storageMgr.mu.Unlock()
			return nil, ErrPartitionManagerStopped
		}

		ptn, ok := storageMgr.partitions[partitionKey]
		if !ok {
			ptn = &partition{
				shutdown: make(chan struct{}),
			}

			stagingDir, err := os.MkdirTemp(storageMgr.stagingDirectory, "")
			if err != nil {
				storageMgr.mu.Unlock()
				return nil, fmt.Errorf("create staging directory: %w", err)
			}

			mgr := NewTransactionManager(storageMgr.database, storageMgr.path, relativePath, stagingDir, storageMgr.repoFactory, storageMgr.transactionFinalizerFactory(ptn))
			ptn.transactionManager = mgr

			storageMgr.partitions[partitionKey] = ptn

			storageMgr.activePartitions.Add(1)
			go func() {
				if err := mgr.Run(); err != nil {
					storageMgr.logger.WithError(err).Error("partition failed")
				}

				// In the event that TransactionManager stops running, a new TransactionManager will
				// need to be started in order to continue processing transactions. The partition is
				// deleted allowing the next transaction for the repository to create a new partition
				// and TransactionManager.
				storageMgr.mu.Lock()
				delete(storageMgr.partitions, partitionKey)
				storageMgr.mu.Unlock()

				close(ptn.shutdown)

				if err := os.RemoveAll(stagingDir); err != nil {
					storageMgr.logger.WithError(err).Error("failed removing partition's staging directory")
				}

				storageMgr.activePartitions.Done()
			}()
		}

		if ptn.shuttingDown {
			// If the partition is in the process of shutting down, the partition should not be
			// used. The lock is released while waiting for the partition to complete shutdown as to
			// not block other partitions from processing transactions. Once shutdown is complete, a
			// new attempt is made to get a valid partition.
			storageMgr.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ptn.shutdown:
			}

			continue
		}

		ptn.pendingTransactionCount++
		storageMgr.mu.Unlock()

		transaction, err := ptn.transactionManager.Begin(ctx)
		if err != nil {
			// The pending transaction count needs to be decremented since the transaction is no longer
			// inflight. A transaction failing does not necessarily mean the transaction manager has
			// stopped running. Consequently, if there are no other pending transactions the partition
			// should be stopped.
			storageMgr.transactionFinalizerFactory(ptn)()

			return nil, err
		}

		return transaction, nil
	}
}

// Stop stops transaction processing for all storages and waits for shutdown completion.
func (pm *PartitionManager) Stop() {
	var activeStorages sync.WaitGroup
	for _, storageMgr := range pm.storages {
		activeStorages.Add(1)
		storageMgr := storageMgr
		go func() {
			storageMgr.stop()
			activeStorages.Done()
		}()
	}

	activeStorages.Wait()
}
