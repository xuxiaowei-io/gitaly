package storagectx

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// Transaction is the interface of the storagemgr.Transaction accessible through the context.
// See the details of that type for method documentation.
type Transaction interface {
	SetDefaultBranch(git.ReferenceName)
	DeleteRepository()
	SetCustomHooks([]byte)
	IncludeObject(git.ObjectID)
	OriginalRepository(*gitalypb.Repository) *gitalypb.Repository
	UpdateAlternate(string)
}

type keyTransaction struct{}

// ContextWithTransaction stores the transaction into the context.
func ContextWithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, keyTransaction{}, tx)
}

// RunWithTransaction runs the callback with the transaction in the context. If there is
// no transaction in the context, the callback is not ran.
func RunWithTransaction(ctx context.Context, callback func(tx Transaction)) {
	value := ctx.Value(keyTransaction{})
	if value == nil {
		return
	}

	callback(value.(Transaction))
}
