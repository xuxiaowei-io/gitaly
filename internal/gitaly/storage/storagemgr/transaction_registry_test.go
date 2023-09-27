package storagemgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

func TestTransactionRegistry(t *testing.T) {
	registry := NewTransactionRegistry()

	expectedTX1 := &Transaction{}
	txID1 := registry.register(expectedTX1)
	require.Equal(t, txID1, storage.TransactionID(1))

	expectedTX2 := &Transaction{}
	txID2 := registry.register(expectedTX2)
	require.Equal(t, txID2, storage.TransactionID(2))

	actualTX, err := registry.Get(txID1)
	require.NoError(t, err)
	require.Same(t, expectedTX1, actualTX)

	registry.unregister(txID1)
	actualTX, err = registry.Get(txID1)
	require.Equal(t, errTransactionNotFound, err)
	require.Nil(t, actualTX)

	actualTX, err = registry.Get(txID2)
	require.NoError(t, err)
	require.Same(t, expectedTX2, actualTX)
}
