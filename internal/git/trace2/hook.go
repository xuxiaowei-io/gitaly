package trace2

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
)

// Hook is the interface for Trace2 hooks
type Hook interface {
	// Name returns the name of the hook
	Name() string
	// Handle is handler function that a hook registers with the manager. After trace tree is
	// built, the manager dispatches handlers in order with the root trace of the tree.
	Handle(context.Context, *Trace, log.Logger) error
}
