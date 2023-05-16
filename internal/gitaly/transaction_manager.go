package gitaly

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/perm"
	"gitlab.com/gitlab-org/gitaly/v16/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// ErrTransactionProcessingStopped is returned when the TransactionManager stops processing transactions.
var ErrTransactionProcessingStopped = errors.New("transaction processing stopped")

// InvalidReferenceFormatError is returned when a reference name was invalid.
type InvalidReferenceFormatError struct {
	// ReferenceName is the reference with invalid format.
	ReferenceName git.ReferenceName
}

// Error returns the formatted error string.
func (err InvalidReferenceFormatError) Error() string {
	return fmt.Sprintf("invalid reference format: %q", err.ReferenceName)
}

// ReferenceVerificationError is returned when a reference's old OID did not match the expected.
type ReferenceVerificationError struct {
	// ReferenceName is the name of the reference that failed verification.
	ReferenceName git.ReferenceName
	// ExpectedOID is the OID the reference was expected to point to.
	ExpectedOID git.ObjectID
	// ActualOID is the OID the reference actually pointed to.
	ActualOID git.ObjectID
}

// Error returns the formatted error string.
func (err ReferenceVerificationError) Error() string {
	return fmt.Sprintf("expected %q to point to %q but it pointed to %q", err.ReferenceName, err.ExpectedOID, err.ActualOID)
}

// ReferenceToBeDeletedError is returned when the reference used is scheduled to be deleted.
type ReferenceToBeDeletedError struct {
	// ReferenceName is the name of the reference that is scheduled to be deleted.
	ReferenceName git.ReferenceName
}

// Error returns the formatted error string.
func (err ReferenceToBeDeletedError) Error() string {
	return fmt.Sprintf("reference %q is scheduled to be deleted", err.ReferenceName)
}

// LogIndex points to a specific position in a repository's write-ahead log.
type LogIndex uint64

// toProto returns the protobuf representation of LogIndex for serialization purposes.
func (index LogIndex) toProto() *gitalypb.LogIndex {
	return &gitalypb.LogIndex{LogIndex: uint64(index)}
}

// String returns a string representation of the LogIndex.
func (index LogIndex) String() string {
	return strconv.FormatUint(uint64(index), 10)
}

// ReferenceUpdate describes the state of a reference's old and new tip in an update.
type ReferenceUpdate struct {
	// OldOID is the old OID the reference is expected to point to prior to updating it.
	// If the reference does not point to the old value, the reference verification fails.
	OldOID git.ObjectID
	// NewOID is the new desired OID to point the reference to.
	NewOID git.ObjectID
}

// DefaultBranchUpdate provides the information to update the default branch of the repo.
type DefaultBranchUpdate struct {
	// Reference is the reference to update the default branch to.
	Reference git.ReferenceName
}

// CustomHooksUpdate models an update to the custom hooks.
type CustomHooksUpdate struct {
	// CustomHooksTAR contains the custom hooks as a TAR. The TAR contains a `custom_hooks`
	// directory which contains the hooks. Setting the update with nil `custom_hooks_tar` clears
	// the hooks from the repository.
	CustomHooksTAR []byte
}

// ReferenceUpdates contains references to update. Reference name is used as the key and the value
// is the expected old tip and the desired new tip.
type ReferenceUpdates map[git.ReferenceName]ReferenceUpdate

// Snapshot contains the read snapshot details of a Transaction.
type Snapshot struct {
	// ReadIndex is the index of the log entry this Transaction is reading the data at.
	ReadIndex LogIndex
	// HookIndex is index of the hooks on the disk that are included in this Transactions's snapshot
	// and were the latest on the read index.
	HookIndex LogIndex
	// HookPath is an absolute filesystem path to the hooks in this snapshot.
	HookPath string
}

// Transaction is a unit-of-work that contains reference changes to perform on the repository.
type Transaction struct {
	// commit commits the Transaction through the TransactionManager.
	commit func(context.Context, *Transaction) error
	// finalize decrements the active transaction count on the partition in the PartitionManager. This is
	// really only a concern for the PartitionManager and will be moved out from here later.
	finalize func()
	// result is where the outcome of the transaction is sent ot by TransactionManager once it
	// has been determined.
	result chan error
	// admitted denotes whether the transaction was admitted for processing in the TransactionManager.
	// Transaction queues in admissionQueue to be committed, and is considered admitted once it has
	// been dequeued by TransactionManager.Run(). Once the transaction is admitted, its ownership moves
	// from the client goroutine to the TransactionManager.Run() goroutine, and the client goroutine must
	// not do any modifications to the state of the transcation anymore to avoid races.
	admitted bool
	// initStagingDirectory is called to lazily initialize the staging directory when it is
	// needed.
	initStagingDirectory func() error
	// stagingDirectory is the directory where the transaction stages its files prior
	// to them being logged. It is cleaned up when the transaction finishes.
	stagingDirectory string
	// quarantineDirectory is the directory within the stagingDirectory where the new objects of the
	// transaction are quarantined.
	quarantineDirectory string
	// includesPack is set if a pack file has been computed for the transaction and should be
	// logged.
	includesPack bool
	// stagingRepository is a repository that is used to stage the transaction. If there are quarantined
	// objects, it has the quarantine applied so the objects are available for verification and packing.
	stagingRepository repository

	// Snapshot contains the details of the Transaction's read snapshot.
	snapshot Snapshot

	skipVerificationFailures bool
	referenceUpdates         ReferenceUpdates
	defaultBranchUpdate      *DefaultBranchUpdate
	customHooksUpdate        *CustomHooksUpdate
}

// Begin opens a new transaction. The caller must call either Commit or Rollback to release
// the resources tied to the transaction. The returned Transaction is not safe for concurrent use.
//
// The returned Transaction's read snapshot includes all writes that were committed prior to the
// Begin call. Begin blocks until the committed writes have been applied to the repository.
func (mgr *TransactionManager) Begin(ctx context.Context) (*Transaction, error) {
	// Wait until the manager has been initialized so the notification channels
	// and the log indexes are loaded.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.initialized:
	}

	mgr.mutex.RLock()
	txn := &Transaction{
		commit:   mgr.commit,
		finalize: mgr.transactionFinalizer,
		snapshot: Snapshot{
			ReadIndex: mgr.appendedLogIndex,
			HookIndex: mgr.hookIndex,
			HookPath:  hookPathForLogIndex(mgr.repositoryPath, mgr.hookIndex),
		},
	}

	// If there are no hooks stored through the WAL yet, then default to the custom hooks
	// that may already exist in the repository for backwards compatibility.
	if txn.snapshot.HookIndex == 0 {
		txn.snapshot.HookPath = filepath.Join(mgr.repositoryPath, "custom_hooks")
	}

	readReady := mgr.applyNotifications[txn.snapshot.ReadIndex]
	mgr.mutex.RUnlock()
	if readReady == nil {
		// The snapshot log entry is already applied if there is no notification channel for it.
		// If so, the transaction is ready to begin immediately.
		readReady = make(chan struct{})
		close(readReady)
	}

	txn.initStagingDirectory = func() error {
		if txn.stagingDirectory != "" {
			return nil
		}

		stagingDirectory, err := os.MkdirTemp(mgr.stagingDirectory, "")
		if err != nil {
			return fmt.Errorf("mkdir temp: %w", err)
		}

		txn.stagingDirectory = stagingDirectory
		return nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.ctx.Done():
		return nil, ErrTransactionProcessingStopped
	case <-readReady:
		return txn, nil
	}
}

// Commit performs the changes. If no error is returned, the transaction was successful and the changes
// have been performed. If an error was returned, the transaction may or may not be persisted.
func (txn *Transaction) Commit(ctx context.Context) (returnedErr error) {
	defer func() {
		txn.finalize()

		if err := txn.cleanUnadmitted(); err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()

	return txn.commit(ctx, txn)
}

// Rollback releases resources associated with the transaction without performing any changes.
func (txn *Transaction) Rollback() error {
	defer txn.finalize()
	return txn.cleanUnadmitted()
}

// cleanUnadmitted cleans up after the transaction if it wasn't yet admitted. If the transaction was admitted,
// the Transaction is being processed by TransactionManager. The clean up responsibility moves there as well
// to avoid races.
func (txn *Transaction) cleanUnadmitted() error {
	if txn.admitted {
		return nil
	}

	return txn.clean()
}

// clean cleans up the resources associated with the transaction.
func (txn *Transaction) clean() error {
	if txn.stagingDirectory != "" {
		if err := os.RemoveAll(txn.stagingDirectory); err != nil {
			return fmt.Errorf("remove staging directory: %w", err)
		}
	}

	return nil
}

// Snapshot returns the details of the Transaction's read snapshot.
func (txn *Transaction) Snapshot() Snapshot {
	return txn.snapshot
}

// SkipVerificationFailures configures the transaction to skip reference updates that fail verification.
// If a reference update fails verification with this set, the update is dropped from the transaction but
// other successful reference updates will be made. By default, the entire transaction is aborted if a
// reference fails verification.
//
// The default behavior models `git push --atomic`. Toggling this option models the behavior without
// the `--atomic` flag.
func (txn *Transaction) SkipVerificationFailures() {
	txn.skipVerificationFailures = true
}

// UpdateReferences updates the given references as part of the transaction. If UpdateReferences is called
// multiple times, only the changes from the latest invocation take place.
func (txn *Transaction) UpdateReferences(updates ReferenceUpdates) {
	txn.referenceUpdates = updates
}

// QuarantineDirectory returns an absolute path to the transaction's quarantine directory. The quarantine directory
// is a Git object directory where the new objects introduced in the transaction must be written. The quarantined
// objects needed by the updated reference tips will be included in the transaction.
func (txn *Transaction) QuarantineDirectory() (string, error) {
	if err := txn.initStagingDirectory(); err != nil {
		return "", fmt.Errorf("init staging directory: %w", err)
	}

	quarantineDirectory := filepath.Join(txn.stagingDirectory, "quarantine")
	if err := os.MkdirAll(filepath.Join(quarantineDirectory, "pack"), perm.PrivateDir); err != nil {
		return "", fmt.Errorf("create quarantine directory: %w", err)
	}

	txn.quarantineDirectory = quarantineDirectory

	return quarantineDirectory, nil
}

// SetDefaultBranch sets the default branch as part of the transaction. If SetDefaultBranch is called
// multiple times, only the changes from the latest invocation take place. The reference is validated
// to exist.
func (txn *Transaction) SetDefaultBranch(new git.ReferenceName) {
	txn.defaultBranchUpdate = &DefaultBranchUpdate{Reference: new}
}

// SetCustomHooks sets the custom hooks as part of the transaction. If SetCustomHooks is called multiple
// times, only the changes from the latest invocation take place. The hooks are extracted as is and are
// not validated. Setting a nil hooksTAR removes the hooks from the repository.
func (txn *Transaction) SetCustomHooks(hooksTAR []byte) {
	txn.customHooksUpdate = &CustomHooksUpdate{CustomHooksTAR: hooksTAR}
}

// packFilePath returns the path to this transaction's pack file.
func (txn *Transaction) packFilePath() string {
	return filepath.Join(txn.stagingDirectory, "transaction.pack")
}

// TransactionManager is responsible for transaction management of a single repository. Each repository has
// a single TransactionManager; it is the repository's single-writer. It accepts writes one at a time from
// the admissionQueue. Each admitted write is processed in three steps:
//
//  1. The references being updated are verified by ensuring the expected old tips match what the references
//     actually point to prior to update. The entire transaction is by default aborted if a single reference
//     fails the verification step. The reference verification behavior can be controlled on a per-transaction
//     level by setting:
//     - The reference verification failures can be ignored instead of aborting the entire transaction.
//     If done, the references that failed verification are dropped from the transaction but the updates
//     that passed verification are still performed.
//     - The reference verification may also be skipped if the write is force updating references. If
//     done, the current state of the references is ignored and they are directly updated to point
//     to the new tips.
//  2. The transaction is appended to the write-ahead log. Once the write has been logged, it is effectively
//     committed and will be applied to the repository even after restarting.
//  3. The transaction is applied from the write-ahead log to the repository by actually performing the reference
//     changes.
//
// The goroutine that issued the transaction is waiting for the result while these steps are being performed. As
// there is no transaction control for readers yet, the issuer is only notified of a successful write after the
// write has been applied to the repository.
//
// TransactionManager recovers transactions after interruptions by applying the write-ahead logged transactions to
// the repository on start up.
//
// TransactionManager maintains the write-ahead log in a key-value store. It maintains the following key spaces:
// - `repository/<repository_id:string>/log/index/applied`
//   - This key stores the index of the log entry that has been applied to the repository. This allows for
//     determining how far a repository is in processing the log and which log entries need to be applied
//     after starting up. Repository starts from log index 0 if there are no log entries recorded to have
//     been applied.
//
// - `repository/<repository_id:string>/log/entry/<log_index:uint64>`
//   - These keys hold the actual write-ahead log entries. A repository's first log entry starts at index 1
//     and the log index keeps monotonically increasing from there on without gaps. The write-ahead log
//     entries are processed in ascending order.
//
// The values in the database are marshaled protocol buffer messages. Numbers in the keys are encoded as big
// endian to preserve the sort order of the numbers also in lexicographical order.
type TransactionManager struct {
	// ctx is the context used for all operations.
	ctx context.Context
	// stop cancels ctx and stops the transaction processing.
	stop context.CancelFunc

	// stopCalled is closed when Stop is called. It unblock transactions that are waiting to be admitted.
	stopCalled <-chan struct{}
	// runDone is closed when Run returns. It unblocks transactions that are waiting for a result after
	// being admitted. This is differentiated from ctx.Done in order to enable testing that Run correctly
	// releases awaiters when the transactions processing is stopped.
	runDone chan struct{}
	// stagingDirectory is a path to a directory where this TransactionManager should stage the files of the transactions
	// before it logs them. The TransactionManager cleans up the files during runtime but stale files may be
	// left around after crashes. The files are temporary and any leftover files are expected to be cleaned up when
	// Gitaly starts.
	stagingDirectory string

	// repository is the repository this TransactionManager is acting on.
	repository repository
	// repositoryPath is the path to the repository this TransactionManager is acting on.
	repositoryPath string
	// relativePath is the repository's relative path inside the storage.
	relativePath string
	// db is the handle to the key-value store used for storing the write-ahead log related state.
	db database
	// admissionQueue is where the incoming writes are waiting to be admitted to the transaction
	// manager.
	admissionQueue chan *Transaction

	// initialized is closed when the manager has been initialized. It's used to block new transactions
	// from beginning prior to the manager having initialized its runtime state on start up.
	initialized chan struct{}
	// mutex guards access to applyNotifications and appendedLogIndex. These fields are accessed by both
	// Run and Begin which are ran in different goroutines.
	mutex sync.RWMutex
	// applyNotifications stores channels that are closed when a log entry is applied. These
	// are used to block transactions from beginning before their snapshot is ready.
	applyNotifications map[LogIndex]chan struct{}
	// appendedLogIndex holds the index of the last log entry appended to the log.
	appendedLogIndex LogIndex
	// appliedLogIndex holds the index of the last log entry applied to the repository
	appliedLogIndex LogIndex
	// hookIndex stores the log index of the latest committed hooks in the repository.
	hookIndex LogIndex

	// transactionFinalizer executes when a transaction is completed.
	transactionFinalizer func()

	// awaitingTransactions contains transactions waiting for their log entry to be applied to
	// the repository. It's keyed by the log index the transaction is waiting to be applied and the
	// value is the resultChannel that is waiting the result.
	awaitingTransactions map[LogIndex]resultChannel
}

// repository is the localrepo interface used by TransactionManager.
type repository interface {
	git.RepositoryExecutor
	ResolveRevision(context.Context, git.Revision) (git.ObjectID, error)
	SetDefaultBranch(ctx context.Context, txManager transaction.Manager, reference git.ReferenceName) error
	UnpackObjects(context.Context, io.Reader) error
	Quarantine(string) (*localrepo.Repo, error)
	WalkUnreachableObjects(context.Context, io.Reader, io.Writer) error
	PackObjects(context.Context, io.Reader, io.Writer) error
}

// NewTransactionManager returns a new TransactionManager for the given repository.
func NewTransactionManager(db *badger.DB, storagePath, relativePath, stagingDir string, repositoryFactory localrepo.StorageScopedFactory, transactionFinalizer func()) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionManager{
		ctx:                  ctx,
		stopCalled:           ctx.Done(),
		runDone:              make(chan struct{}),
		stop:                 cancel,
		repository:           repositoryFactory.Build(relativePath),
		repositoryPath:       filepath.Join(storagePath, relativePath),
		relativePath:         relativePath,
		db:                   newDatabaseAdapter(db),
		admissionQueue:       make(chan *Transaction),
		initialized:          make(chan struct{}),
		applyNotifications:   make(map[LogIndex]chan struct{}),
		stagingDirectory:     stagingDir,
		transactionFinalizer: transactionFinalizer,
		awaitingTransactions: make(map[LogIndex]resultChannel),
	}
}

// resultChannel represents a future that will yield the result of a transaction once its
// outcome has been decided.
type resultChannel chan error

// commit queues the transaction for processing and returns once the result has been determined.
func (mgr *TransactionManager) commit(ctx context.Context, transaction *Transaction) error {
	transaction.result = make(resultChannel, 1)

	if err := mgr.setupStagingRepository(ctx, transaction); err != nil {
		return fmt.Errorf("setup staging repository: %w", err)
	}

	if err := mgr.packObjects(ctx, transaction); err != nil {
		return fmt.Errorf("pack objects: %w", err)
	}

	select {
	case mgr.admissionQueue <- transaction:
		transaction.admitted = true

		select {
		case err := <-transaction.result:
			return unwrapExpectedError(err)
		case <-ctx.Done():
			return ctx.Err()
		case <-mgr.runDone:
			return ErrTransactionProcessingStopped
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-mgr.stopCalled:
		return ErrTransactionProcessingStopped
	}
}

// setupStagingRepository sets a repository that is used to stage the transaction. The staging repository
// has the quarantine applied so the objects are available for packing and verifying the references.
func (mgr *TransactionManager) setupStagingRepository(ctx context.Context, transaction *Transaction) error {
	// If the repository exists, we use it for staging the transaction.
	transaction.stagingRepository = mgr.repository

	// If the transaction has a quarantine directory, we must use it when staging the pack
	// file and verifying the references so the objects are available.
	if transaction.quarantineDirectory != "" {
		quarantinedRepo, err := transaction.stagingRepository.Quarantine(transaction.quarantineDirectory)
		if err != nil {
			return fmt.Errorf("quarantine: %w", err)
		}

		transaction.stagingRepository = quarantinedRepo
	}

	return nil
}

// packObjects packs the objects included in the transaction into a single pack file that is ready
// for logging. The pack file includes all unreachable objects that are about to be made reachable.
func (mgr *TransactionManager) packObjects(ctx context.Context, transaction *Transaction) error {
	if transaction.quarantineDirectory == "" {
		return nil
	}

	objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	heads := make([]string, 0, len(transaction.referenceUpdates))
	for _, update := range transaction.referenceUpdates {
		if update.NewOID == objectHash.ZeroOID {
			// Reference deletions can't introduce new objects so ignore them.
			continue
		}

		heads = append(heads, update.NewOID.String())
	}

	if len(heads) == 0 {
		// No need to pack objects if there are no changes that can introduce new objects.
		return nil
	}

	transaction.includesPack = true

	objectsReader, objectsWriter := io.Pipe()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() (returnedErr error) {
		defer func() { objectsWriter.CloseWithError(returnedErr) }()

		if err := transaction.stagingRepository.WalkUnreachableObjects(ctx,
			strings.NewReader(strings.Join(heads, "\n")),
			objectsWriter,
		); err != nil {
			return fmt.Errorf("walk objects: %w", err)
		}

		return nil
	})

	group.Go(func() (returnedErr error) {
		defer func() { objectsReader.CloseWithError(returnedErr) }()

		destinationFile, err := os.OpenFile(
			transaction.packFilePath(),
			os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			perm.PrivateFile,
		)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer destinationFile.Close()

		if err := transaction.stagingRepository.PackObjects(ctx, objectsReader, destinationFile); err != nil {
			return fmt.Errorf("pack objects: %w", err)
		}

		// Sync the contents of the pack so they are flushed to disk prior to the transaction
		// being admitted for processing.
		if err := destinationFile.Sync(); err != nil {
			return fmt.Errorf("sync pack: %w", err)
		}

		return destinationFile.Close()
	})

	return group.Wait()
}

// unwrapExpectedError unwraps expected errors that may occur and returns them directly to the caller.
func unwrapExpectedError(err error) error {
	// The manager controls its own execution context and it is canceled only when Stop is called.
	// Any context.Canceled errors returned are thus from shutting down so we report that here.
	if errors.Is(err, context.Canceled) {
		return ErrTransactionProcessingStopped
	}

	return err
}

// Run starts the transaction processing. On start up Run loads the indexes of the last appended and applied
// log entries from the database. It will then apply any transactions that have been logged but not applied
// to the repository. Once the recovery is completed, Run starts processing new transactions by verifying the
// references, logging the transaction and finally applying it to the repository. The transactions are acknowledged
// once they've been applied to the repository.
//
// Run keeps running until Stop is called or it encounters a fatal error. All transactions will error with
// ErrTransactionProcessingStopped when Run returns.
func (mgr *TransactionManager) Run() (returnedErr error) {
	defer func() {
		// On-going operations may fail with a context canceled error if the manager is stopped. This is
		// not a real error though given the manager will recover from this on restart. Swallow the error.
		if errors.Is(returnedErr, context.Canceled) {
			returnedErr = nil
		}
	}()

	// Defer the Stop in order to release all on-going Commit calls in case of error.
	defer close(mgr.runDone)
	defer mgr.Stop()

	if err := mgr.initialize(mgr.ctx); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	for {
		if mgr.appliedLogIndex < mgr.appendedLogIndex {
			logIndex := mgr.appliedLogIndex + 1

			if err := mgr.applyLogEntry(mgr.ctx, logIndex); err != nil {
				return fmt.Errorf("apply log entry: %w", err)
			}

			continue
		}

		if err := mgr.processTransaction(); err != nil {
			return fmt.Errorf("process transaction: %w", err)
		}
	}
}

// processTransaction waits for a transaction and processes it by verifying and
// logging it.
func (mgr *TransactionManager) processTransaction() (returnedErr error) {
	var cleanUps []func() error
	defer func() {
		for _, cleanUp := range cleanUps {
			if err := cleanUp(); err != nil && returnedErr == nil {
				returnedErr = fmt.Errorf("clean up: %w", err)
			}
		}
	}()

	var transaction *Transaction
	select {
	case transaction = <-mgr.admissionQueue:
		// The Transaction does not clean up itself anymore once it has been admitted for
		// processing. This avoids the Transaction concurrently removing the staged state
		// while the manager is still operating on it. We thus need to defer its clean up.
		cleanUps = append(cleanUps, transaction.clean)
	case <-mgr.ctx.Done():
	}

	// Return if the manager was stopped. The select is indeterministic so this guarantees
	// the manager stops the processing even if there are transactions in the queue.
	if err := mgr.ctx.Err(); err != nil {
		return err
	}

	if err := func() (commitErr error) {
		logEntry := &gitalypb.LogEntry{}

		var err error
		logEntry.ReferenceUpdates, err = mgr.verifyReferences(mgr.ctx, transaction)
		if err != nil {
			return fmt.Errorf("verify references: %w", err)
		}

		if transaction.defaultBranchUpdate != nil {
			if err := mgr.verifyDefaultBranchUpdate(mgr.ctx, transaction); err != nil {
				return fmt.Errorf("verify default branch update: %w", err)
			}

			logEntry.DefaultBranchUpdate = &gitalypb.LogEntry_DefaultBranchUpdate{
				ReferenceName: []byte(transaction.defaultBranchUpdate.Reference),
			}
		}

		if transaction.customHooksUpdate != nil {
			logEntry.CustomHooksUpdate = &gitalypb.LogEntry_CustomHooksUpdate{
				CustomHooksTar: transaction.customHooksUpdate.CustomHooksTAR,
			}
		}
		nextLogIndex := mgr.appendedLogIndex + 1

		if transaction.includesPack {
			logEntry.IncludesPack = true

			removePack, err := mgr.storePackFile(mgr.ctx, nextLogIndex, transaction)
			cleanUps = append(cleanUps, func() error {
				// The transaction's pack file might have been moved in to place at <repo>/wal/packs/<log_index>.pack.
				// If anything fails before the transaction is committed, the pack file must be removed as otherwise
				// it would occupy the pack file slot of the next log entry. If this can't be done, the TransactionManager
				// will exit with an error. The pack file will be cleaned up on restart and no further processing is
				// allowed until that happens.
				if commitErr != nil {
					return removePack()
				}

				return nil
			})

			if err != nil {
				return fmt.Errorf("store pack file: %w", err)
			}
		}

		return mgr.appendLogEntry(nextLogIndex, logEntry)
	}(); err != nil {
		transaction.result <- err
		return nil
	}

	mgr.awaitingTransactions[mgr.appendedLogIndex] = transaction.result

	return nil
}

// Stop stops the transaction processing causing Run to return.
func (mgr *TransactionManager) Stop() { mgr.stop() }

// initialize initializes the TransactionManager's state from the database. It loads the appendend and the applied
// indexes and initializes the notification channels that synchronize transaction beginning with log entry applying.
func (mgr *TransactionManager) initialize(ctx context.Context) error {
	defer close(mgr.initialized)

	if err := mgr.createDirectories(); err != nil {
		return fmt.Errorf("create directories: %w", err)
	}

	var appliedLogIndex gitalypb.LogIndex
	if err := mgr.readKey(keyAppliedLogIndex(mgr.relativePath), &appliedLogIndex); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("read applied log index: %w", err)
	}

	mgr.appliedLogIndex = LogIndex(appliedLogIndex.LogIndex)

	// The index of the last appended log entry is determined from the indexes of the latest entry in the log and
	// the latest applied log entry. If there is a log entry, it is the latest appended log entry. If there are no
	// log entries, the latest log entry must have been applied to the repository and pruned away, meaning the index
	// of the last appended log entry is the same as the index if the last applied log entry.
	//
	// As the log indexes in the keys are encoded in big endian, the latest log entry can be found by taking
	// the first key when iterating the log entry key space in reverse.
	if err := mgr.db.View(func(txn databaseTransaction) error {
		logPrefix := keyPrefixLogEntries(mgr.relativePath)

		iterator := txn.NewIterator(badger.IteratorOptions{Reverse: true, Prefix: logPrefix})
		defer iterator.Close()

		mgr.appendedLogIndex = mgr.appliedLogIndex

		// The iterator seeks to a key that is greater than or equal than seeked key. Since we are doing a reverse
		// seek, we need to add 0xff to the prefix so the first iterated key is the latest log entry.
		if iterator.Seek(append(logPrefix, 0xff)); iterator.Valid() {
			mgr.appendedLogIndex = LogIndex(binary.BigEndian.Uint64(bytes.TrimPrefix(iterator.Item().Key(), logPrefix)))
		}

		return nil
	}); err != nil {
		return fmt.Errorf("determine appended log index: %w", err)
	}

	var err error
	mgr.hookIndex, err = mgr.determineHookIndex(ctx, mgr.appendedLogIndex, mgr.appliedLogIndex)
	if err != nil {
		return fmt.Errorf("determine hook index: %w", err)
	}

	// Each unapplied log entry should have a notification channel that gets closed when it is applied.
	// Create these channels here for the log entries.
	for i := mgr.appliedLogIndex + 1; i <= mgr.appendedLogIndex; i++ {
		mgr.applyNotifications[i] = make(chan struct{})
	}

	if err := mgr.removeStalePackFiles(mgr.ctx, mgr.appendedLogIndex); err != nil {
		return fmt.Errorf("remove stale packs: %w", err)
	}

	return nil
}

// determineHookIndex determines the latest hooks in the repository.
//
//  1. First we iterate through the unapplied log in reverse order. The first log entry that
//     contains custom hooks must have the latest hooks since it is the latest log entry.
//  2. If we don't find any custom hooks in the log, the latest hooks could have been applied
//     to the repository already and the log entry pruned away. Look at the hooks on the disk
//     to see which are the latest.
//  3. If we found no hooks in the log nor in the repository, there are no hooks configured.
func (mgr *TransactionManager) determineHookIndex(ctx context.Context, appendedIndex, appliedIndex LogIndex) (LogIndex, error) {
	for i := appendedIndex; appliedIndex < i; i-- {
		logEntry, err := mgr.readLogEntry(i)
		if err != nil {
			return 0, fmt.Errorf("read log entry: %w", err)
		}

		if logEntry.CustomHooksUpdate != nil {
			return i, nil
		}
	}

	hookDirs, err := os.ReadDir(filepath.Join(mgr.repositoryPath, "wal", "hooks"))
	if err != nil {
		return 0, fmt.Errorf("read hook directories: %w", err)
	}

	var hookIndex LogIndex
	for _, dir := range hookDirs {
		rawIndex, err := strconv.ParseUint(dir.Name(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse hook index: %w", err)
		}

		if index := LogIndex(rawIndex); hookIndex < index {
			hookIndex = index
		}
	}

	return hookIndex, err
}

// createDirectories creates the directories that are expected to exist
// in the repository for storing the state. Initializing them simplifies
// rest of the code as it doesn't need handling for when they don't.
func (mgr *TransactionManager) createDirectories() error {
	for _, relativePath := range []string{
		"wal/hooks",
		"wal/packs",
	} {
		directory := filepath.Join(mgr.repositoryPath, relativePath)
		if _, err := os.Stat(directory); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stat directory: %w", err)
			}

			if err := os.MkdirAll(directory, fs.ModePerm); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}

			if err := safe.NewSyncer().SyncHierarchy(mgr.repositoryPath, relativePath); err != nil {
				return fmt.Errorf("sync: %w", err)
			}
		}
	}

	return nil
}

// removeStalePackFiles removes pack files from the log directory that have no associated log entry.
// Such packs can be left around if a transaction's pack file was moved in place successfully
// but the manager was interrupted before successfully persisting the log entry itself.
func (mgr *TransactionManager) removeStalePackFiles(ctx context.Context, appendedIndex LogIndex) error {
	// Log entries are appended one by one to the log. If a write is interrupted, the only possible stale
	// pack would be for the next log index. Remove the pack if it exists.
	possibleStalePackPath := packFilePathForLogIndex(mgr.repositoryPath, appendedIndex+1)
	if err := os.Remove(possibleStalePackPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove: %w", err)
		}

		return nil
	}

	// Sync the parent directory to flush the file deletion.
	if err := safe.NewSyncer().Sync(filepath.Dir(possibleStalePackPath)); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	return nil
}

// storePackFile moves the transaction's pack file from the object directory to its destination in the log.
// It returns a function, even on errors, that must be called to clean up the pack file committing the log entry
// fails.
func (mgr *TransactionManager) storePackFile(ctx context.Context, index LogIndex, transaction *Transaction) (func() error, error) {
	removePack := func() error { return nil }

	destinationPath := packFilePathForLogIndex(mgr.repositoryPath, index)
	if err := os.Rename(
		transaction.packFilePath(),
		destinationPath,
	); err != nil {
		return removePack, fmt.Errorf("move pack file: %w", err)
	}

	removePack = func() error {
		if err := os.Remove(destinationPath); err != nil {
			return fmt.Errorf("remove pack file: %w", err)
		}

		return nil
	}

	// Sync the parent directory. The pack's contents are synced when the pack file is computed.
	if err := safe.NewSyncer().Sync(filepath.Dir(destinationPath)); err != nil {
		return removePack, fmt.Errorf("sync: %w", err)
	}

	return removePack, nil
}

// packFilePathForLogIndex returns a log entry's pack file's absolute path in a given
// a repository path.
func packFilePathForLogIndex(repoPath string, index LogIndex) string {
	return filepath.Join(repoPath, "wal", "packs", index.String()+".pack")
}

// verifyReferences verifies that the references in the transaction apply on top of the already accepted
// reference changes. The old tips in the transaction are verified against the current actual tips.
// It returns the write-ahead log entry for the transaction if it was successfully verified.
func (mgr *TransactionManager) verifyReferences(ctx context.Context, transaction *Transaction) ([]*gitalypb.LogEntry_ReferenceUpdate, error) {
	var referenceUpdates []*gitalypb.LogEntry_ReferenceUpdate
	for referenceName, tips := range transaction.referenceUpdates {
		// 'git update-ref' doesn't ensure the loose references end up in the
		// refs directory so we enforce that here.
		if !strings.HasPrefix(referenceName.String(), "refs/") {
			return nil, InvalidReferenceFormatError{ReferenceName: referenceName}
		}

		// We'll later implement reference format verification in Gitaly. update-ref reports errors with these characters
		// in a difficult to parse manner. For now, let's check these two illegal characters separately so we can return a
		// proper error.
		for _, illegalCharacter := range []byte{0, '\n'} {
			if bytes.Contains([]byte(referenceName), []byte{illegalCharacter}) {
				return nil, InvalidReferenceFormatError{ReferenceName: referenceName}
			}
		}

		actualOldTip, err := transaction.stagingRepository.ResolveRevision(ctx, referenceName.Revision())
		if errors.Is(err, git.ErrReferenceNotFound) {
			objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
			if err != nil {
				return nil, fmt.Errorf("object hash: %w", err)
			}

			actualOldTip = objectHash.ZeroOID
		} else if err != nil {
			return nil, fmt.Errorf("resolve revision: %w", err)
		}

		if tips.OldOID != actualOldTip {
			if transaction.skipVerificationFailures {
				continue
			}

			return nil, ReferenceVerificationError{
				ReferenceName: referenceName,
				ExpectedOID:   tips.OldOID,
				ActualOID:     actualOldTip,
			}
		}

		referenceUpdates = append(referenceUpdates, &gitalypb.LogEntry_ReferenceUpdate{
			ReferenceName: []byte(referenceName),
			NewOid:        []byte(tips.NewOID),
		})
	}

	// Sort the reference updates so the reference changes are always logged in a deterministic order.
	sort.Slice(referenceUpdates, func(i, j int) bool {
		return bytes.Compare(
			referenceUpdates[i].ReferenceName,
			referenceUpdates[j].ReferenceName,
		) == -1
	})

	if err := mgr.verifyReferencesWithGit(ctx, referenceUpdates, transaction.stagingRepository); err != nil {
		return nil, fmt.Errorf("verify references with git: %w", err)
	}

	return referenceUpdates, nil
}

// vefifyReferencesWithGit verifies the reference updates with git by preparing reference transaction. This ensures
// the updates will go through when they are being applied in the log. This also catches any invalid reference names
// and file/directory conflicts with Git's loose reference storage which can occur with references like
// 'refs/heads/parent' and 'refs/heads/parent/child'.
func (mgr *TransactionManager) verifyReferencesWithGit(ctx context.Context, referenceUpdates []*gitalypb.LogEntry_ReferenceUpdate, stagingRepository repository) error {
	updater, err := mgr.prepareReferenceTransaction(ctx, referenceUpdates, stagingRepository)
	if err != nil {
		return fmt.Errorf("prepare reference transaction: %w", err)
	}

	return updater.Close()
}

// verifyDefaultBranchUpdate verifies the default branch referance update. This is done by first checking if it is one of
// the references in the current transaction which is not scheduled to be deleted. If not, we check if its a valid reference
// name in the repository. We don't do reference name validation because any reference going through the transaction manager
// has name validation and we can rely on that.
func (mgr *TransactionManager) verifyDefaultBranchUpdate(ctx context.Context, transaction *Transaction) error {
	referenceName := transaction.defaultBranchUpdate.Reference

	// Check the transaction reference updates, to see if the refname exists, if we find it here
	// we don't have to invoke git to do a refname check.
	if refUpdate, ok := transaction.referenceUpdates[referenceName]; ok {
		objectHash, err := transaction.stagingRepository.ObjectHash(ctx)
		if err != nil {
			return fmt.Errorf("obtaining object hash: %w", err)
		}

		// reference is scheduled to be deleted
		if refUpdate.NewOID == objectHash.ZeroOID {
			return ReferenceToBeDeletedError{ReferenceName: referenceName}
		}

		return nil
	}

	if _, err := transaction.stagingRepository.ResolveRevision(ctx, referenceName.Revision()); err != nil {
		return fmt.Errorf("cannot resolve default branch update: %w", err)
	}

	return nil
}

// updateDefaultBranch sets the default branch using localrepo.SetDefaultBranch if there is adequate datprovided.
func (mgr *TransactionManager) updateDefaultBranch(ctx context.Context, defaultBranch *gitalypb.LogEntry_DefaultBranchUpdate) error {
	if defaultBranch == nil {
		return nil
	}

	return mgr.repository.SetDefaultBranch(ctx, nil, git.ReferenceName(defaultBranch.ReferenceName))
}

// prepareReferenceTransaction prepares a reference transaction with `git update-ref`. It leaves committing
// or aborting up to the caller. Either should be called to clean up the process. The process is cleaned up
// if an error is returned.
func (mgr *TransactionManager) prepareReferenceTransaction(ctx context.Context, referenceUpdates []*gitalypb.LogEntry_ReferenceUpdate, repository repository) (*updateref.Updater, error) {
	updater, err := updateref.New(ctx, repository, updateref.WithDisabledTransactions())
	if err != nil {
		return nil, fmt.Errorf("new: %w", err)
	}

	if err := updater.Start(); err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	for _, referenceUpdate := range referenceUpdates {
		if err := updater.Update(git.ReferenceName(referenceUpdate.ReferenceName), git.ObjectID(referenceUpdate.NewOid), ""); err != nil {
			return nil, fmt.Errorf("update %q: %w", referenceUpdate.ReferenceName, err)
		}
	}

	if err := updater.Prepare(); err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}

	return updater, nil
}

// appendLogEntry appends the transaction to the write-ahead log. References that failed verification are skipped and thus not
// logged nor applied later.
func (mgr *TransactionManager) appendLogEntry(nextLogIndex LogIndex, logEntry *gitalypb.LogEntry) error {
	if err := mgr.storeLogEntry(nextLogIndex, logEntry); err != nil {
		return fmt.Errorf("set log entry: %w", err)
	}

	mgr.mutex.Lock()
	mgr.appendedLogIndex = nextLogIndex
	if logEntry.CustomHooksUpdate != nil {
		mgr.hookIndex = nextLogIndex
	}
	mgr.applyNotifications[nextLogIndex] = make(chan struct{})
	mgr.mutex.Unlock()

	return nil
}

// applyLogEntry reads a log entry at the given index and applies it to the repository.
func (mgr *TransactionManager) applyLogEntry(ctx context.Context, logIndex LogIndex) (returnedErr error) {
	logEntry, err := mgr.readLogEntry(logIndex)
	if err != nil {
		return fmt.Errorf("read log entry: %w", err)
	}

	if logEntry.IncludesPack {
		if err := mgr.applyPackFile(ctx, logIndex); err != nil {
			return fmt.Errorf("apply pack file: %w", err)
		}
	}

	updater, err := mgr.prepareReferenceTransaction(ctx, logEntry.ReferenceUpdates, mgr.repository)
	if err != nil {
		return fmt.Errorf("perpare reference transaction: %w", err)
	}

	if err := updater.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	if err := mgr.updateDefaultBranch(ctx, logEntry.DefaultBranchUpdate); err != nil {
		return fmt.Errorf("writing default branch: %w", err)
	}

	if err := mgr.applyCustomHooks(ctx, logIndex, logEntry.CustomHooksUpdate); err != nil {
		return fmt.Errorf("apply custom hooks: %w", err)
	}

	if err := mgr.storeAppliedLogIndex(logIndex); err != nil {
		return fmt.Errorf("set applied log index: %w", err)
	}

	if err := mgr.deleteLogEntry(logIndex); err != nil {
		return fmt.Errorf("deleting log entry: %w", err)
	}

	mgr.appliedLogIndex = logIndex

	// Notify the transactions waiting for this log entry to be applied prior to beginning.
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	notificationCh, ok := mgr.applyNotifications[logIndex]
	if !ok {
		// This should never happen and is a programming error if it does.
		return fmt.Errorf("no notification channel for LSN %d", logIndex)
	}
	delete(mgr.applyNotifications, logIndex)
	close(notificationCh)

	// There is no awaiter for a transaction if the transaction manager is recovering
	// transactions from the log after starting up.
	if resultChan, ok := mgr.awaitingTransactions[logIndex]; ok {
		resultChan <- nil
		delete(mgr.awaitingTransactions, logIndex)
	}

	return nil
}

// applyPackFile unpacks the objects from the pack file into the repository if the log entry
// has an associated pack file.
func (mgr *TransactionManager) applyPackFile(ctx context.Context, logIndex LogIndex) error {
	packFile, err := os.Open(packFilePathForLogIndex(mgr.repositoryPath, logIndex))
	if err != nil {
		return fmt.Errorf("open pack file: %w", err)
	}
	defer packFile.Close()

	return mgr.repository.UnpackObjects(ctx, packFile)
}

// applyCustomHooks applies the custom hooks to the repository from the log entry. The hooks are stored
// at `<repo>/wal/hooks/<log_index>`. The hooks are fsynced prior to returning so it is safe to delete
// the log entry afterwards.
func (mgr *TransactionManager) applyCustomHooks(ctx context.Context, logIndex LogIndex, update *gitalypb.LogEntry_CustomHooksUpdate) error {
	if update == nil {
		return nil
	}

	targetDirectory := hookPathForLogIndex(mgr.repositoryPath, logIndex)
	if err := os.Mkdir(targetDirectory, fs.ModePerm); err != nil {
		// The target directory may exist if we previously tried to extract the
		// hooks there. TAR overwrites existing files and the hooks files are
		// guaranteed to be the same as this is the same log entry.
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create directory: %w", err)
		}
	}

	if err := repoutil.ExtractHooks(ctx, bytes.NewReader(update.CustomHooksTar), targetDirectory, true); err != nil {
		return fmt.Errorf("extract hooks: %w", err)
	}

	syncer := safe.NewSyncer()
	// TAR doesn't sync the extracted files so do it manually here.
	if err := syncer.SyncRecursive(targetDirectory); err != nil {
		return fmt.Errorf("sync hooks: %w", err)
	}

	// Sync the parent directory as well.
	if err := syncer.SyncParent(targetDirectory); err != nil {
		return fmt.Errorf("sync hook directory: %w", err)
	}

	return nil
}

// hookPathForLogIndex returns the filesystem paths where the hooks for the
// given log index are stored.
func hookPathForLogIndex(repositoryPath string, logIndex LogIndex) string {
	return filepath.Join(repositoryPath, "wal", "hooks", logIndex.String())
}

// deleteLogEntry deletes the log entry at the given index from the log.
func (mgr *TransactionManager) deleteLogEntry(index LogIndex) error {
	return mgr.deleteKey(keyLogEntry(mgr.relativePath, index))
}

// readLogEntry returns the log entry from the given position in the log.
func (mgr *TransactionManager) readLogEntry(index LogIndex) (*gitalypb.LogEntry, error) {
	var logEntry gitalypb.LogEntry
	key := keyLogEntry(mgr.relativePath, index)

	if err := mgr.readKey(key, &logEntry); err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}

	return &logEntry, nil
}

// storeLogEntry stores the log entry in the repository's write-ahead log at the given index.
func (mgr *TransactionManager) storeLogEntry(index LogIndex, entry *gitalypb.LogEntry) error {
	return mgr.setKey(keyLogEntry(mgr.relativePath, index), entry)
}

// storeAppliedLogIndex stores the repository's applied log index in the database.
func (mgr *TransactionManager) storeAppliedLogIndex(index LogIndex) error {
	return mgr.setKey(keyAppliedLogIndex(mgr.relativePath), index.toProto())
}

// setKey marshals and stores a given protocol buffer message into the database under the given key.
func (mgr *TransactionManager) setKey(key []byte, value proto.Message) error {
	marshaledValue, err := proto.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	writeBatch := mgr.db.NewWriteBatch()
	defer writeBatch.Cancel()

	if err := writeBatch.Set(key, marshaledValue); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	return writeBatch.Flush()
}

// readKey reads a key from the database and unmarshals its value in to the destination protocol
// buffer message.
func (mgr *TransactionManager) readKey(key []byte, destination proto.Message) error {
	return mgr.db.View(func(txn databaseTransaction) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return item.Value(func(value []byte) error { return proto.Unmarshal(value, destination) })
	})
}

// deleteKey deletes a key from the database.
func (mgr *TransactionManager) deleteKey(key []byte) error {
	return mgr.db.Update(func(txn databaseTransaction) error {
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("delete: %w", err)
		}

		return nil
	})
}

// keyAppliedLogIndex returns the database key storing a repository's last applied log entry's index.
func keyAppliedLogIndex(repositoryID string) []byte {
	return []byte(fmt.Sprintf("repository/%s/log/index/applied", repositoryID))
}

// keyLogEntry returns the database key storing a repository's log entry at a given index.
func keyLogEntry(repositoryID string, index LogIndex) []byte {
	marshaledIndex := make([]byte, binary.Size(index))
	binary.BigEndian.PutUint64(marshaledIndex, uint64(index))
	return []byte(fmt.Sprintf("%s%s", keyPrefixLogEntries(repositoryID), marshaledIndex))
}

// keyPrefixLogEntries returns the key prefix holding repository's write-ahead log entries.
func keyPrefixLogEntries(repositoryID string) []byte {
	return []byte(fmt.Sprintf("repository/%s/log/entry/", repositoryID))
}
