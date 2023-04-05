package gitaly

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/safe"
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
}

// Transaction is a unit-of-work that contains reference changes to perform on the repository.
type Transaction struct {
	// commit commits the Transaction through the TransactionManager.
	commit func(context.Context, *Transaction) error
	// result is where the outcome of the transaction is sent ot by TransactionManager once it
	// has been determined.
	result chan error

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
	snapshot := Snapshot{
		ReadIndex: mgr.appendedLogIndex,
		HookIndex: mgr.hookIndex,
	}
	readReady := mgr.applyNotifications[snapshot.ReadIndex]
	mgr.mutex.RUnlock()
	if readReady == nil {
		// The snapshot log entry is already applied if there is no notification channel for it.
		// If so, the transaction is ready to begin immediately.
		readReady = make(chan struct{})
		close(readReady)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mgr.ctx.Done():
		return nil, ErrTransactionProcessingStopped
	case <-readReady:
		return &Transaction{
			commit:   mgr.commit,
			snapshot: snapshot,
		}, nil
	}
}

// Commit performs the changes. If no error is returned, the transaction was successful and the changes
// have been performed. If an error was returned, the transaction may or may not be persisted.
func (txn *Transaction) Commit(ctx context.Context) error {
	return txn.commit(ctx, txn)
}

// Rollback releases resources associated with the transaction without performing any changes.
func (txn *Transaction) Rollback() {}

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
}

// repository is the localrepo interface used by TransactionManager.
type repository interface {
	git.RepositoryExecutor
	ResolveRevision(context.Context, git.Revision) (git.ObjectID, error)
	SetDefaultBranch(ctx context.Context, txManager transaction.Manager, reference git.ReferenceName) error
	Path() (string, error)
}

// NewTransactionManager returns a new TransactionManager for the given repository.
func NewTransactionManager(db *badger.DB, repository *localrepo.Repo) *TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionManager{
		ctx:                ctx,
		stopCalled:         ctx.Done(),
		runDone:            make(chan struct{}),
		stop:               cancel,
		repository:         repository,
		db:                 newDatabaseAdapter(db),
		admissionQueue:     make(chan *Transaction),
		initialized:        make(chan struct{}),
		applyNotifications: make(map[LogIndex]chan struct{}),
	}
}

// resultChannel represents a future that will yield the result of a transaction once its
// outcome has been decided.
type resultChannel chan error

// commit queus the transaction for processing and returns once the result has been determined.
func (mgr *TransactionManager) commit(ctx context.Context, transaction *Transaction) error {
	transaction.result = make(resultChannel, 1)

	select {
	case mgr.admissionQueue <- transaction:
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
func (mgr *TransactionManager) processTransaction() error {
	var transaction *Transaction
	select {
	case transaction = <-mgr.admissionQueue:
	case <-mgr.ctx.Done():
	}

	// Return if the manager was stopped. The select is indeterministic so this guarantees
	// the manager stops the processing even if there are transactions in the queue.
	if err := mgr.ctx.Err(); err != nil {
		return err
	}

	transaction.result <- func() error {
		logEntry, err := mgr.verifyReferences(mgr.ctx, transaction)
		if err != nil {
			return fmt.Errorf("verify references: %w", err)
		}

		if transaction.customHooksUpdate != nil {
			logEntry.CustomHooksUpdate = &gitalypb.LogEntry_CustomHooksUpdate{
				CustomHooksTar: transaction.customHooksUpdate.CustomHooksTAR,
			}
		}

		return mgr.appendLogEntry(logEntry)
	}()

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
	if err := mgr.db.View(func(txn databaseTransaction) error {
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

	repoPath, err := mgr.repository.Path()
	if err != nil {
		return 0, fmt.Errorf("repository path: %w", err)
	}

	hookDirs, err := os.ReadDir(filepath.Join(repoPath, "wal", "hooks"))
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
	repoPath, err := mgr.repository.Path()
	if err != nil {
		return fmt.Errorf("repo path: %w", err)
	}

	for _, relativePath := range []string{
		"wal/hooks",
	} {
		directory := filepath.Join(repoPath, relativePath)
		if _, err := os.Stat(directory); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stat directory: %w", err)
			}

			if err := os.MkdirAll(directory, fs.ModePerm); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}

			if err := safe.NewSyncer().SyncHierarchy(repoPath, relativePath); err != nil {
				return fmt.Errorf("sync: %w", err)
			}
		}
	}

	return nil
}

// verifyReferences verifies that the references in the transaction apply on top of the already accepted
// reference changes. The old tips in the transaction are verified against the current actual tips.
// It returns the write-ahead log entry for the transaction if it was successfully verified.
func (mgr *TransactionManager) verifyReferences(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry, error) {
	logEntry := &gitalypb.LogEntry{}
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
			if transaction.skipVerificationFailures {
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

	if transaction.defaultBranchUpdate != nil {
		if err := mgr.verifyDefaultBranchUpdate(ctx, transaction); err != nil {
			return nil, fmt.Errorf("verify default branch update: %w", err)
		}

		logEntry.DefaultBranchUpdate = &gitalypb.LogEntry_DefaultBranchUpdate{
			ReferenceName: []byte(transaction.defaultBranchUpdate.Reference),
		}
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

// verifyDefaultBranchUpdate verifies the default branch referance update. This is done by first checking if it is one of
// the references in the current transaction which is not scheduled to be deleted. If not, we check if its a valid reference
// name in the repository. We don't do reference name validation because any reference going through the transaction manager
// has name validation and we can rely on that.
func (mgr *TransactionManager) verifyDefaultBranchUpdate(ctx context.Context, transaction *Transaction) error {
	referenceName := transaction.defaultBranchUpdate.Reference

	// Check the transaction reference updates, to see if the refname exists, if we find it here
	// we don't have to invoke git to do a refname check.
	if refUpdate, ok := transaction.referenceUpdates[referenceName]; ok {
		objectHash, err := mgr.repository.ObjectHash(ctx)
		if err != nil {
			return fmt.Errorf("obtaining object hash: %w", err)
		}

		// reference is scheduled to be deleted
		if refUpdate.NewOID == objectHash.ZeroOID {
			return ReferenceToBeDeletedError{ReferenceName: referenceName}
		}

		return nil
	}

	if _, err := mgr.repository.ResolveRevision(ctx, referenceName.Revision()); err != nil {
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
func (mgr *TransactionManager) applyLogEntry(ctx context.Context, logIndex LogIndex) error {
	logEntry, err := mgr.readLogEntry(logIndex)
	if err != nil {
		return fmt.Errorf("read log entry: %w", err)
	}

	updater, err := mgr.prepareReferenceTransaction(ctx, logEntry.ReferenceUpdates)
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
	notificationCh, ok := mgr.applyNotifications[logIndex]
	if !ok {
		// This should never happen and is a programming error if it does.
		return fmt.Errorf("no notification channel for LSN %d", logIndex)
	}
	delete(mgr.applyNotifications, logIndex)
	mgr.mutex.Unlock()
	close(notificationCh)

	return nil
}

// applyCustomHooks applies the custom hooks to the repository from the log entry. The hooks are stored
// at `<repo>/wal/hooks/<log_index>`. The hooks are fsynced prior to returning so it is safe to delete
// the log entry afterwards.
func (mgr *TransactionManager) applyCustomHooks(ctx context.Context, logIndex LogIndex, update *gitalypb.LogEntry_CustomHooksUpdate) error {
	if update == nil {
		return nil
	}

	repoPath, err := mgr.repository.Path()
	if err != nil {
		return fmt.Errorf("repository path: %w", err)
	}

	syncer := safe.NewSyncer()

	hooksPath := filepath.Join("wal", "hooks")
	targetDirectory := filepath.Join(repoPath, hooksPath, logIndex.String())
	if err := os.Mkdir(targetDirectory, fs.ModePerm); err != nil {
		// The target directory may exist if we previously tried to extract the
		// hooks there. TAR overwrites existing files and the hooks files are
		// guaranteed to be the same as this is the same log entry.
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create directory: %w", err)
		}
	}

	if err := hook.ExtractHooks(ctx, bytes.NewReader(update.CustomHooksTar), targetDirectory, true); err != nil {
		return fmt.Errorf("extract hooks: %w", err)
	}

	// TAR doesn't sync the extracted files so do it manually here.
	if err := syncer.SyncRecursive(targetDirectory); err != nil {
		return fmt.Errorf("sync hooks: %w", err)
	}

	// Sync the parent directory as well.
	if err := syncer.Sync(filepath.Join(repoPath, hooksPath)); err != nil {
		return fmt.Errorf("sync hook directory: %w", err)
	}

	return nil
}

// deleteLogEntry deletes the log entry at the given index from the log.
func (mgr *TransactionManager) deleteLogEntry(index LogIndex) error {
	return mgr.deleteKey(keyLogEntry(getRepositoryID(mgr.repository), index))
}

// readLogEntry returns the log entry from the given position in the log.
func (mgr *TransactionManager) readLogEntry(index LogIndex) (*gitalypb.LogEntry, error) {
	var logEntry gitalypb.LogEntry
	key := keyLogEntry(getRepositoryID(mgr.repository), index)

	if err := mgr.readKey(key, &logEntry); err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}

	return &logEntry, nil
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
