package gitaly

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
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

// LogIndex points to a specific position in a repository's write-ahead log.
type LogIndex uint64

// toProto returns the protobuf representation of LogIndex for serialization purposes.
func (index LogIndex) toProto() *gitalypb.LogIndex {
	return &gitalypb.LogIndex{LogIndex: uint64(index)}
}

// ReferenceUpdate describes the state of a reference's old and new tip in an update.
type ReferenceUpdate struct {
	// OldOID is the old OID the reference is expected to point to prior to updating it.
	// If the reference does not point to the old value, the reference verification fails.
	OldOID git.ObjectID
	// NewOID is the new desired OID to point the reference to.
	NewOID git.ObjectID
}

// ReferenceUpdates contains references to update. Reference name is used as the key and the value
// is the expected old tip and the desired new tip.
type ReferenceUpdates map[git.ReferenceName]ReferenceUpdate

// Transaction is a unit-of-work that contains reference changes to perform on the repository.
type Transaction struct {
	// SkipVerificationFailures causes references updates that failed the verification step to be
	// dropped from the transaction. By default, any verification failures abort the entire transaction.
	//
	// The default behavior maps to the `--atomic` flag of `git push`. When this is set, the behavior matches
	// what happens git's behavior without the flag.
	SkipVerificationFailures bool
	// ReferenceUpdates contains the reference updates to be performed.
	ReferenceUpdates
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

	// repository is the repository this TransactionManager is acting on.
	repository repository
	// db is the handle to the key-value store used for storing the write-ahead log related state.
	db database
	// admissionQueue is where the incoming writes are waiting to be admitted to the transaction
	// manager.
	admissionQueue chan transactionFuture

	// appendedLogIndex holds the index of the last log entry appended to the log.
	appendedLogIndex LogIndex
	// appliedLogIndex holds the index of the last log entry applied to the repository
	appliedLogIndex LogIndex
}

// repository is the localrepo interface used by TransactionManager.
type repository interface {
	git.RepositoryExecutor
	ResolveRevision(context.Context, git.Revision) (git.ObjectID, error)
}

// NewTransactionManager returns a new TransactionManager for the given repository.
func NewTransactionManager(db *badger.DB, repository *localrepo.Repo) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionManager{
		ctx:            ctx,
		stopCalled:     ctx.Done(),
		runDone:        make(chan struct{}),
		stop:           cancel,
		repository:     repository,
		db:             newDatabaseAdapter(db),
		admissionQueue: make(chan transactionFuture),
	}
}

// resultChannel represents a future that will yield the result of a transaction once its
// outcome has been decided.
type resultChannel chan error

// transactionFuture holds a transaction and the resultChannel the transaction queuer is waiting
// for the result on.
type transactionFuture struct {
	transaction Transaction
	result      resultChannel
}

// Propose queues a transaction for the TransactionManager to process. Propose returns once the transaction
// has been successfully applied to the repository.
//
// Transaction is not committed if any of the following errors is returned:
// - InvalidReferenceFormatError is returned when attempting to update a reference with an invalid name.
// - ReferenceVerificationError is returned when the reference does not point to the expected old tip.
//
// Transaction may or may not be committed if any of the following errors is returned:
//   - ErrTransactionProcessing stopped is returned when the TransactionManager is closing and stops processing
//     transactions.
//   - Unexpected error occurs.
func (mgr *TransactionManager) Propose(ctx context.Context, transaction Transaction) error {
	queuedTransaction := transactionFuture{transaction: transaction, result: make(resultChannel, 1)}

	select {
	case mgr.admissionQueue <- queuedTransaction:
		select {
		case err := <-queuedTransaction.result:
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
// Run keeps running until Stop is called or it encounters a fatal error. All active Propose calls return
// ErrTransactionProcessingStopped when Run returns.
func (mgr *TransactionManager) Run() error {
	// Defer the Stop in order to release all on-going Propose calls in case of error.
	defer close(mgr.runDone)
	defer mgr.Stop()

	if err := mgr.initialize(); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	// awaitingTransactions contains transactions waiting for their log entry to be applied to
	// the repository. It's keyed by the log index the transaction is waiting to be applied and the
	// value is the resultChannel that is waiting the result.
	awaitingTransactions := make(map[LogIndex]resultChannel)

	for {
		if mgr.appliedLogIndex < mgr.appendedLogIndex {
			logIndex := mgr.appliedLogIndex + 1

			if err := mgr.applyLogEntry(mgr.ctx, logIndex); err != nil {
				return fmt.Errorf("apply log entry: %w", err)
			}

			// There is no awaiter for a transaction if the transaction manager is recovering
			// transactions from the log after starting up.
			if resultChan, ok := awaitingTransactions[logIndex]; ok {
				resultChan <- nil
				delete(awaitingTransactions, logIndex)
			}

			continue
		}

		var transaction transactionFuture
		select {
		case transaction = <-mgr.admissionQueue:
		case <-mgr.ctx.Done():
		}

		// Return if the manager was stopped. The select is indeterministic so this guarantees
		// the manager stops the processing even if there are transactions in the queue.
		if mgr.ctx.Err() != nil {
			return nil
		}

		if err := func() error {
			logEntry, err := mgr.verifyReferences(mgr.ctx, transaction.transaction)
			if err != nil {
				return fmt.Errorf("verify references: %w", err)
			}

			return mgr.appendLogEntry(logEntry)
		}(); err != nil {
			transaction.result <- err
			continue
		}

		awaitingTransactions[mgr.appendedLogIndex] = transaction.result
	}
}

// Stop stops the transaction processing causing Run to return.
func (mgr *TransactionManager) Stop() { mgr.stop() }

// initialize initializes the TransactionManager by loading the applied log index and determining the
// appended log index from the database.
func (mgr *TransactionManager) initialize() error {
	var appliedLogIndex gitalypb.LogIndex
	if err := mgr.readKey(keyAppliedLogIndex(getRepositoryID(mgr.repository)), &appliedLogIndex); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
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
	return mgr.db.View(func(txn databaseTransaction) error {
		logPrefix := keyPrefixLogEntries(getRepositoryID(mgr.repository))

		iterator := txn.NewIterator(badger.IteratorOptions{Reverse: true, Prefix: logPrefix})
		defer iterator.Close()

		mgr.appendedLogIndex = mgr.appliedLogIndex

		// The iterator seeks to a key that is greater than or equal than seeked key. Since we are doing a reverse
		// seek, we need to add 0xff to the prefix so the first iterated key is the latest log entry.
		if iterator.Seek(append(logPrefix, 0xff)); iterator.Valid() {
			mgr.appendedLogIndex = LogIndex(binary.BigEndian.Uint64(bytes.TrimPrefix(iterator.Item().Key(), logPrefix)))
		}

		return nil
	})
}

// verifyReferences verifies that the references in the transaction apply on top of the already accepted
// reference changes. The old tips in the transaction are verified against the current actual tips.
// It returns the write-ahead log entry for the transaction if it was successfully verified.
func (mgr *TransactionManager) verifyReferences(ctx context.Context, transaction Transaction) (*gitalypb.LogEntry, error) {
	logEntry := &gitalypb.LogEntry{}
	for referenceName, tips := range transaction.ReferenceUpdates {
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

		actualOldTip, err := mgr.repository.ResolveRevision(ctx, referenceName.Revision())
		if errors.Is(err, git.ErrReferenceNotFound) {
			objectHash, err := mgr.repository.ObjectHash(ctx)
			if err != nil {
				return nil, fmt.Errorf("object hash: %w", err)
			}

			actualOldTip = objectHash.ZeroOID
		} else if err != nil {
			return nil, fmt.Errorf("resolve revision: %w", err)
		}

		if tips.OldOID != actualOldTip {
			if transaction.SkipVerificationFailures {
				continue
			}

			return nil, ReferenceVerificationError{
				ReferenceName: referenceName,
				ExpectedOID:   tips.OldOID,
				ActualOID:     actualOldTip,
			}
		}

		logEntry.ReferenceUpdates = append(logEntry.ReferenceUpdates, &gitalypb.LogEntry_ReferenceUpdate{
			ReferenceName: []byte(referenceName),
			NewOid:        []byte(tips.NewOID),
		})
	}

	// Sort the reference updates so the reference changes are always logged in a deterministic order.
	sort.Slice(logEntry.ReferenceUpdates, func(i, j int) bool {
		return bytes.Compare(
			logEntry.ReferenceUpdates[i].ReferenceName,
			logEntry.ReferenceUpdates[j].ReferenceName,
		) == -1
	})

	if err := mgr.verifyReferencesWithGit(ctx, logEntry.ReferenceUpdates); err != nil {
		return nil, fmt.Errorf("verify references with git: %w", err)
	}

	return logEntry, nil
}

// vefifyReferencesWithGit verifies the reference updates with git by preparing reference transaction. This ensures
// the updates will go through when they are being applied in the log. This also catches any invalid reference names
// and file/directory conflicts with Git's loose reference storage which can occur with references like
// 'refs/heads/parent' and 'refs/heads/parent/child'.
func (mgr *TransactionManager) verifyReferencesWithGit(ctx context.Context, referenceUpdates []*gitalypb.LogEntry_ReferenceUpdate) error {
	updater, err := mgr.prepareReferenceTransaction(ctx, referenceUpdates)
	if err != nil {
		return fmt.Errorf("prepare reference transaction: %w", err)
	}

	return updater.Close()
}

// prepareReferenceTransaction prepares a reference transaction with `git update-ref`. It leaves committing
// or aborting up to the caller. Either should be called to clean up the process. The process is cleaned up
// if an error is returned.
func (mgr *TransactionManager) prepareReferenceTransaction(ctx context.Context, referenceUpdates []*gitalypb.LogEntry_ReferenceUpdate) (*updateref.Updater, error) {
	updater, err := updateref.New(ctx, mgr.repository, updateref.WithDisabledTransactions())
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
func (mgr *TransactionManager) appendLogEntry(logEntry *gitalypb.LogEntry) error {
	nextLogIndex := mgr.appendedLogIndex + 1

	if err := mgr.storeLogEntry(nextLogIndex, logEntry); err != nil {
		return fmt.Errorf("set log entry: %w", err)
	}

	mgr.appendedLogIndex = nextLogIndex

	return nil
}

// applyLogEntry reads a log entry at the given index and applies it to the repository.
func (mgr *TransactionManager) applyLogEntry(ctx context.Context, logIndex LogIndex) error {
	var logEntry gitalypb.LogEntry
	key := keyLogEntry(getRepositoryID(mgr.repository), logIndex)

	if err := mgr.readKey(key, &logEntry); err != nil {
		return fmt.Errorf("read log entry: %w", err)
	}

	updater, err := mgr.prepareReferenceTransaction(ctx, logEntry.ReferenceUpdates)
	if err != nil {
		return fmt.Errorf("perpare reference transaction: %w", err)
	}

	if err := updater.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	if err := mgr.storeAppliedLogIndex(logIndex); err != nil {
		return fmt.Errorf("set applied log index: %w", err)
	}

	if err := mgr.deleteKey(key); err != nil {
		return fmt.Errorf("deleting log entry: %w", err)
	}

	mgr.appliedLogIndex = logIndex

	return nil
}

// storeLogEntry stores the log entry in the repository's write-ahead log at the given index.
func (mgr *TransactionManager) storeLogEntry(index LogIndex, entry *gitalypb.LogEntry) error {
	return mgr.setKey(keyLogEntry(getRepositoryID(mgr.repository), index), entry)
}

// storeAppliedLogIndex stores the repository's applied log index in the database.
func (mgr *TransactionManager) storeAppliedLogIndex(index LogIndex) error {
	return mgr.setKey(keyAppliedLogIndex(getRepositoryID(mgr.repository)), index.toProto())
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

// getRepositoryID returns a repository's ID. The ID should never change as it is used in the database
// keys. Gitaly does not have a permanent ID to use yet so the repository's storage name and relative
// path are used as a composite key.
func getRepositoryID(repository repository) string {
	return repository.GetStorageName() + ":" + repository.GetRelativePath()
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
