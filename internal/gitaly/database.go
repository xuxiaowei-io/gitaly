package gitaly

import (
	"github.com/dgraph-io/badger/v3"
)

// OpenDatabase opens a new database handle to a database at the given path.
func OpenDatabase(databasePath string) (*badger.DB, error) {
	dbOptions := badger.DefaultOptions(databasePath)
	// Enable SyncWrites to ensure all writes are persisted to disk before considering
	// them committed.
	dbOptions.SyncWrites = true
	// Badger by default logs fairly verbose statistics when opening a database. Disable the
	// logging for now.
	dbOptions.Logger = nil

	return badger.Open(dbOptions)
}

// databaseAdapter adapts a *badger.DB to the internal database interface used by the hooks in tests.
type databaseAdapter struct{ *badger.DB }

// newDatabaseAdapter adapts a *badger.DB to conform to the internal database interface used for
// hooking into during testing.
func newDatabaseAdapter(db *badger.DB) database {
	return databaseAdapter{DB: db}
}

// NewWriteBatch calls badger.*DB.NewWriteBatch. Refer to Badger's documentation for details.
func (db databaseAdapter) NewWriteBatch() writeBatch {
	return db.DB.NewWriteBatch()
}

// View calls badger.*DB.View. Refer to Badger's documentation for details.
func (db databaseAdapter) View(handler func(databaseTransaction) error) error {
	return db.DB.View(func(txn *badger.Txn) error { return handler(txn) })
}

// Update calls badger.*DB.View. Refer to Badger's documentation for details.
func (db databaseAdapter) Update(handler func(databaseTransaction) error) error {
	return db.DB.Update(func(txn *badger.Txn) error { return handler(txn) })
}

// database is the Badger.DB interface used by TransactionManager. Refer to Badger's documentation
// for details.
type database interface {
	NewWriteBatch() writeBatch
	View(func(databaseTransaction) error) error
	Update(func(databaseTransaction) error) error
}

// writeBatch is the interface of Badger.WriteBatch used by TransactionManager. Refer to Badger's
// documentation for details.
type writeBatch interface {
	Set([]byte, []byte) error
	Flush() error
	Cancel()
}

// databaseTransaction is the interface of *Badger.Txn used by TransactionManager. Refer to Badger's
// documentation for details
type databaseTransaction interface {
	Get([]byte) (*badger.Item, error)
	Delete([]byte) error
	NewIterator(badger.IteratorOptions) *badger.Iterator
}
