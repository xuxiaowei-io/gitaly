package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestContextWithTransactionID(t *testing.T) {
	t.Run("no transaction id in context", func(t *testing.T) {
		require.Equal(t,
			TransactionID(0),
			ExtractTransactionID(testhelper.Context(t)),
		)
	})

	t.Run("transaction id in context", func(t *testing.T) {
		require.Equal(t,
			TransactionID(1),
			ExtractTransactionID(
				ContextWithTransactionID(testhelper.Context(t), 1),
			),
		)
	})
}
