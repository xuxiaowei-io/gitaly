package gitaly

import (
	"context"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
)

// hookFunc is a function that is executed at a specific point. It gets a hookContext that allows it to
// influence the execution of the test.
type hookFunc func(hookContext)

// hookContext are the control toggels available in a hook.
type hookContext struct {
	// stopManager calls the calls stops the TransactionManager.
	stopManager func()
	// database provides access to the database for the hook handler.
	database *badger.DB
	tb       testing.TB
}

// hooks are functions that get invoked at specific points of the TransactionManager Run method. They allow
// for hooking into the Run method at specific poins which would otherwise to do assertions that would otherwise
// not be possible.
type hooks struct {
	// beforeReadLogEntry is invoked before a log entry is read from the database.
	beforeReadLogEntry hookFunc
	// beforeResolveRevision is invoked before ResolveRevision is invoked.
	beforeResolveRevision hookFunc
	// beforeDeferredStop is invoked before the deferred Stop is invoked in Run.
	beforeDeferredStop hookFunc
	// beforeDeleteLogEntry is invoked before a log entry is deleted from the database.
	beforeDeleteLogEntry hookFunc
}

// installHooks installs the configured hooks into the transactionManager.
func installHooks(tb testing.TB, transactionManager *TransactionManager, database *badger.DB, repository *localrepo.Repo, hooks hooks) {
	hookContext := hookContext{stopManager: transactionManager.stop, database: database, tb: &testingHook{TB: tb}}

	transactionManager.stop = func() {
		programCounter, _, _, ok := runtime.Caller(2)
		require.True(tb, ok)

		isDeferredStopInRun := strings.HasSuffix(
			runtime.FuncForPC(programCounter).Name(),
			"gitaly.(*TransactionManager).Run",
		)

		if isDeferredStopInRun && hooks.beforeDeferredStop != nil {
			hooks.beforeDeferredStop(hookContext)
		}

		hookContext.stopManager()
	}

	transactionManager.db = databaseHook{
		database:    newDatabaseAdapter(database),
		hooks:       hooks,
		hookContext: hookContext,
	}

	transactionManager.repository = repositoryHook{
		repository:  repository,
		hookContext: hookContext,
		hooks:       hooks,
	}
}

type repositoryHook struct {
	repository
	hookContext
	hooks
}

func (hook repositoryHook) ResolveRevision(ctx context.Context, revision git.Revision) (git.ObjectID, error) {
	if hook.beforeResolveRevision != nil {
		hook.hooks.beforeResolveRevision(hook.hookContext)
	}

	return hook.repository.ResolveRevision(ctx, revision)
}

type databaseHook struct {
	database
	hookContext
	hooks
}

func (hook databaseHook) View(handler func(databaseTransaction) error) error {
	return hook.database.View(func(transaction databaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) Update(handler func(databaseTransaction) error) error {
	return hook.database.Update(func(transaction databaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) NewWriteBatch() writeBatch {
	return writeBatchHook{writeBatch: hook.database.NewWriteBatch()}
}

type databaseTransactionHook struct {
	databaseTransaction
	hookContext
	hooks
}

var regexLogEntry = regexp.MustCompile("repository/.+/log/entry/")

func (hook databaseTransactionHook) Get(key []byte) (*badger.Item, error) {
	if regexLogEntry.Match(key) {
		if hook.hooks.beforeReadLogEntry != nil {
			hook.hooks.beforeReadLogEntry(hook.hookContext)
		}
	}

	return hook.databaseTransaction.Get(key)
}

func (hook databaseTransactionHook) NewIterator(options badger.IteratorOptions) *badger.Iterator {
	return hook.databaseTransaction.NewIterator(options)
}

func (hook databaseTransactionHook) Delete(key []byte) error {
	if regexLogEntry.Match(key) && hook.beforeDeleteLogEntry != nil {
		hook.beforeDeleteLogEntry(hook.hookContext)
	}

	return hook.databaseTransaction.Delete(key)
}

type writeBatchHook struct {
	writeBatch
}

func (hook writeBatchHook) Set(key []byte, value []byte) error {
	return hook.writeBatch.Set(key, value)
}

func (hook writeBatchHook) Flush() error { return hook.writeBatch.Flush() }

func (hook writeBatchHook) Cancel() { hook.writeBatch.Cancel() }

type testingHook struct {
	testing.TB
}

// We override the FailNow call to the regular testing.Fail, so that it can be
// used within goroutines without replacing calls made to the `require` library.
func (t testingHook) FailNow() {
	t.Fail()
}
