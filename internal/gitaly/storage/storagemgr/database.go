package storagemgr

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// OpenDatabase opens a new database handle to a database at the given path.
func OpenDatabase(logger log.Logger, databasePath string) (*badger.DB, error) {
	dbOptions := badger.DefaultOptions(databasePath)
	// Enable SyncWrites to ensure all writes are persisted to disk before considering
	// them committed.
	dbOptions.SyncWrites = true
	dbOptions.Logger = badgerLogger{logger}

	return badger.Open(dbOptions)
}

type badgerLogger struct {
	log.Logger
}

func (l badgerLogger) Debugf(msg string, args ...any) {
	l.Debug(fmt.Sprintf(msg, args...))
}

func (l badgerLogger) Infof(msg string, args ...any) {
	l.Info(fmt.Sprintf(msg, args...))
}

func (l badgerLogger) Warningf(msg string, args ...any) {
	l.Warn(fmt.Sprintf(msg, args...))
}

func (l badgerLogger) Errorf(msg string, args ...any) {
	l.Error(fmt.Sprintf(msg, args...))
}

// databaseAdapter adapts a *badger.DB to the internal database interface used by the hooks in tests.
type databaseAdapter struct{ *badger.DB }

// newDatabaseAdapter adapts a *badger.DB to conform to the internal database interface used for
// hooking into during testing.
func newDatabaseAdapter(db *badger.DB) Database {
	return databaseAdapter{DB: db}
}

// NewWriteBatch calls badger.*DB.NewWriteBatch. Refer to Badger's documentation for details.
func (db databaseAdapter) NewWriteBatch() WriteBatch {
	return db.DB.NewWriteBatch()
}

// View calls badger.*DB.View. Refer to Badger's documentation for details.
func (db databaseAdapter) View(handler func(DatabaseTransaction) error) error {
	return db.DB.View(func(txn *badger.Txn) error { return handler(txn) })
}

// Update calls badger.*DB.View. Refer to Badger's documentation for details.
func (db databaseAdapter) Update(handler func(DatabaseTransaction) error) error {
	return db.DB.Update(func(txn *badger.Txn) error { return handler(txn) })
}

// Database is the Badger.DB interface used by TransactionManager. Refer to Badger's documentation
// for details.
type Database interface {
	NewWriteBatch() WriteBatch
	View(func(DatabaseTransaction) error) error
	Update(func(DatabaseTransaction) error) error
}

// WriteBatch is the interface of Badger.WriteBatch used by TransactionManager. Refer to Badger's
// documentation for details.
type WriteBatch interface {
	Set([]byte, []byte) error
	Flush() error
	Cancel()
}

// DatabaseTransaction is the interface of *Badger.Txn used by TransactionManager. Refer to Badger's
// documentation for details
type DatabaseTransaction interface {
	Get([]byte) (*badger.Item, error)
	Delete([]byte) error
	NewIterator(badger.IteratorOptions) *badger.Iterator
}
