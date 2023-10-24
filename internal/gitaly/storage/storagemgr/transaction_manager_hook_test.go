package storagemgr

import (
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// hookFunc is a function that is executed at a specific point. It gets a hookContext that allows it to
// influence the execution of the test.
type hookFunc func(hookContext)

// hookContext are the control toggels available in a hook.
type hookContext struct {
	// closeManager calls the calls Close on the TransactionManager.
	closeManager func()
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
	// beforeStoreLogEntry is invoked before the log entry is stored to the database.
	beforeStoreLogEntry hookFunc
	// beforeDeferredClose is invoked before the deferred Close is invoked in Run.
	beforeDeferredClose hookFunc
	// beforeDeleteLogEntry is invoked before a log entry is deleted from the database.
	beforeDeleteLogEntry hookFunc
	// beforeReadAppliedLSN is invoked before the applied LSN is read from the database.
	beforeReadAppliedLSN hookFunc
	// beforeStoreAppliedLSN is invoked before a the applied LSN is stored.
	beforeStoreAppliedLSN hookFunc
}

// installHooks installs the configured hooks into the transactionManager.
func installHooks(tb testing.TB, transactionManager *TransactionManager, database *badger.DB, hooks hooks) {
	hookContext := hookContext{closeManager: transactionManager.close, database: database, tb: &testingHook{TB: tb}}

	transactionManager.close = func() {
		programCounter, _, _, ok := runtime.Caller(2)
		require.True(tb, ok)

		isDeferredCloseInRun := strings.HasSuffix(
			runtime.FuncForPC(programCounter).Name(),
			"gitaly.(*TransactionManager).Run",
		)

		if isDeferredCloseInRun && hooks.beforeDeferredClose != nil {
			hooks.beforeDeferredClose(hookContext)
		}

		hookContext.closeManager()
	}

	transactionManager.db = databaseHook{
		Database:    newDatabaseAdapter(database),
		hooks:       hooks,
		hookContext: hookContext,
	}
}

type databaseHook struct {
	Database
	hookContext
	hooks
}

func (hook databaseHook) View(handler func(DatabaseTransaction) error) error {
	return hook.Database.View(func(transaction DatabaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) Update(handler func(DatabaseTransaction) error) error {
	return hook.Database.Update(func(transaction DatabaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) NewWriteBatch() WriteBatch {
	return writeBatchHook{
		writeBatch:  hook.database.NewWriteBatch(),
		hookContext: hook.hookContext,
		hooks:       hook.hooks,
	}
}

type databaseTransactionHook struct {
	databaseTransaction DatabaseTransaction
	hookContext
	hooks
}

var (
	regexLogEntry   = regexp.MustCompile("partition/.+/log/entry/")
	regexAppliedLSN = regexp.MustCompile("partition/.+/applied_lsn")
)

func (hook databaseTransactionHook) Get(key []byte) (*badger.Item, error) {
	if regexLogEntry.Match(key) {
		if hook.hooks.beforeReadLogEntry != nil {
			hook.hooks.beforeReadLogEntry(hook.hookContext)
		}
	} else if regexAppliedLSN.Match(key) {
		if hook.hooks.beforeReadAppliedLSN != nil {
			hook.hooks.beforeReadAppliedLSN(hook.hookContext)
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
	writeBatch WriteBatch
	hookContext
	hooks
}

func (hook writeBatchHook) Set(key []byte, value []byte) error {
	if regexAppliedLSN.Match(key) && hook.hooks.beforeStoreAppliedLSN != nil {
		hook.hooks.beforeStoreAppliedLSN(hook.hookContext)
	}

	if regexLogEntry.Match(key) && hook.hooks.beforeStoreLogEntry != nil {
		hook.hooks.beforeStoreLogEntry(hook.hookContext)
	}

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
