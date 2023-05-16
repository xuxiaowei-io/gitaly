package hook

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// DisabledManager never executes hooks and simply returns a nil error.
type DisabledManager struct{}

// PreReceiveHook ignores its parameters and returns a nil error.
func (DisabledManager) PreReceiveHook(context.Context, *gitaly.Transaction, *gitalypb.Repository, []string, []string, io.Reader, io.Writer, io.Writer) error {
	return nil
}

// PostReceiveHook ignores its parameters and returns a nil error.
func (DisabledManager) PostReceiveHook(context.Context, *gitaly.Transaction, *gitalypb.Repository, []string, []string, io.Reader, io.Writer, io.Writer) error {
	return nil
}

// UpdateHook ignores its parameters and returns a nil error.
func (DisabledManager) UpdateHook(context.Context, *gitaly.Transaction, *gitalypb.Repository, string, string, string, []string, io.Writer, io.Writer) error {
	return nil
}

// ReferenceTransactionHook ignores its parameters and returns a nil error.
func (DisabledManager) ReferenceTransactionHook(context.Context, ReferenceTransactionState, []string, io.Reader) error {
	return nil
}
