package txutil

import (
	"errors"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

type rollbackerFunc func() error

func (fn rollbackerFunc) Rollback() error { return fn() }

func TestLogRollback(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		ctx := ctxlogrus.ToContext(testhelper.Context(t), logrus.NewEntry(logger))

		LogRollback(ctx, rollbackerFunc(func() error { return nil }))

		require.Empty(t, hook.AllEntries())
	})

	t.Run("error", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		ctx := ctxlogrus.ToContext(testhelper.Context(t), logrus.NewEntry(logger))

		expectedErr := errors.New("expected error")
		LogRollback(ctx, rollbackerFunc(func() error { return expectedErr }))

		require.Len(t, hook.AllEntries(), 1)
		require.Equal(t, "failed rolling back transaction", hook.LastEntry().Message)
		require.Equal(t, logrus.Fields{logrus.ErrorKey: expectedErr}, hook.LastEntry().Data)
		require.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
	})
}
