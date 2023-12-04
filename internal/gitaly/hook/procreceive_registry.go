package hook

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// ReferenceUpdate denotes a single reference update to be made.
type ReferenceUpdate struct {
	Ref    git.ReferenceName
	OldOID git.ObjectID
	NewOID git.ObjectID
}

// ProcReceiveHookInvocation is an interface which provides abstraction
// around the proc-receive invocation provided by the ProcReceiveRegistry.
// The interface allows the users to obtain reference updates, meta
// information around these updates and functions to accept or reject
// individual updates.
type ProcReceiveHookInvocation interface {
	// Atomic denotes whether the push was atomic.
	Atomic() bool

	// ReferenceUpdates provides the reference updates to be made.
	ReferenceUpdates() []ReferenceUpdate

	// AcceptUpdate writes to the stream that the reference was accepted.
	AcceptUpdate(referenceName git.ReferenceName) error
	// RejectUpdate writes to the stream the reference was rejected and
	// the reason why.
	RejectUpdate(referenceName git.ReferenceName, reason string) error

	// Close must be called on the invocation to clean up. Calling it also
	// signals to the `ProcReceiveHook` handler that it can exit as the streams
	// are no longer needed.
	Close() error
}

type procReceiveHookInvocation struct {
	acceptUpdateFn   func(referenceName git.ReferenceName) error
	rejectUpdateFn   func(referenceName git.ReferenceName, reason string) error
	closeFn          func() error
	referenceUpdates []ReferenceUpdate
	id               storage.TransactionID
	atomic           bool
}

func newProcReceiveHookInvocation(
	atomic bool,
	id storage.TransactionID,
	referenceUpdates []ReferenceUpdate,
	acceptUpdateFn func(referenceName git.ReferenceName) error,
	rejectUpdateFn func(referenceName git.ReferenceName, reason string) error,
	closeFn func() error,
) *procReceiveHookInvocation {
	return &procReceiveHookInvocation{
		atomic:           atomic,
		id:               id,
		referenceUpdates: referenceUpdates,
		acceptUpdateFn:   acceptUpdateFn,
		rejectUpdateFn:   rejectUpdateFn,
		closeFn:          closeFn,
	}
}

// Atomic denotes whether the push was atomic.
func (i *procReceiveHookInvocation) Atomic() bool {
	return i.atomic
}

// ReferenceUpdates provides the reference updates to be made.
func (i *procReceiveHookInvocation) ReferenceUpdates() []ReferenceUpdate {
	return i.referenceUpdates
}

// AcceptUpdate writes to the stream that the reference was accepted.
func (i *procReceiveHookInvocation) AcceptUpdate(referenceName git.ReferenceName) error {
	return i.acceptUpdateFn(referenceName)
}

// RejectUpdate writes to the stream the reference was rejected and
// the reason why.
func (i *procReceiveHookInvocation) RejectUpdate(referenceName git.ReferenceName, reason string) error {
	return i.rejectUpdateFn(referenceName, reason)
}

// Close must be called on the invocation to clean up. Calling it also
// signals to the `ProcReceiveHook` handler that it can exit as the streams
// are no longer needed.
func (i *procReceiveHookInvocation) Close() error {
	return i.closeFn()
}

// ProcReceiveRegistry is the registry which provides the proc-receive hook
// invocation mechanism against a provided transaction ID.
//
// The registry allows RPCs to communicate with the git-proc-receive hook and
// receive information about the reference updates to be performed, then the RPCs
// can interact with the transaction manager and accept or reject reference
// updates, this information is relayed to the proc-receive hook which will
// relay the information to the user.
type ProcReceiveRegistry struct {
	subs        map[storage.TransactionID]chan ProcReceiveHookInvocation
	invocations map[storage.TransactionID]ProcReceiveHookInvocation
	sync.Mutex
}

// NewProcReceiveRegistry creates a new registry by allocating the required
// variables.
func NewProcReceiveRegistry() *ProcReceiveRegistry {
	return &ProcReceiveRegistry{
		subs:        make(map[storage.TransactionID]chan ProcReceiveHookInvocation),
		invocations: make(map[storage.TransactionID]ProcReceiveHookInvocation),
	}
}

// Get is a blocking call which allows the user to obtain the invocation for
// a particular transaction ID. If the proc-receive hook is yet to add the
// invocation, the call blocks indefinitely until it is available.
//
// Once an invocation is retrieved, it is deleted from the internal state.
func (r *ProcReceiveRegistry) Get(id storage.TransactionID) ProcReceiveHookInvocation {
	r.Lock()

	if invocation, ok := r.invocations[id]; ok {
		delete(r.invocations, id)
		r.Unlock()

		return invocation
	}

	ch := make(chan ProcReceiveHookInvocation)
	r.subs[id] = ch
	r.Unlock()

	return <-ch
}

// set adds a invocation against its transaction ID. If there are any
// subscribers waiting for this invocation, we stream the invocation to them
// and unblock them.
func (r *ProcReceiveRegistry) set(invocation *procReceiveHookInvocation) {
	r.Lock()
	defer r.Unlock()

	r.invocations[invocation.id] = invocation

	if listener, ok := r.subs[invocation.id]; ok {
		listener <- invocation
	}
	delete(r.subs, invocation.id)
}
