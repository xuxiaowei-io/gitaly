package txutil

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
)

// LogRollback rolls back the transaction and logs any possible error.
func LogRollback(ctx context.Context, tx interface{ Rollback() error }) {
	if err := tx.Rollback(); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Error("failed rolling back transaction")
	}
}
