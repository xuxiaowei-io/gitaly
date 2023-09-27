package storage

import (
	"context"
)

// TransactionID is an ID that uniquely identifies a Transaction.
type TransactionID uint64

// keyTransactionID is the context key storing a TransactionID.
type keyTransactionID struct{}

// ContextWithTransactionID stores the transaction id in the context.
func ContextWithTransactionID(ctx context.Context, id TransactionID) context.Context {
	return context.WithValue(ctx, keyTransactionID{}, id)
}

// ExtractTransactionID extracts the transaction ID from the context. The returned ID is zero
// if there was no transaction ID in the context.
func ExtractTransactionID(ctx context.Context) TransactionID {
	value := ctx.Value(keyTransactionID{})
	if value == nil {
		return 0
	}

	return value.(TransactionID)
}
