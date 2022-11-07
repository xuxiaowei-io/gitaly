package catfile

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// ObjectReader returns information about an object referenced by a given revision.
type ObjectReader interface {
	cacheable

	// Info requests information about the revision pointed to by the given revision.
	Info(context.Context, git.Revision) (*ObjectInfo, error)

	// Reader returns a new Object for the given revision. The Object must be fully consumed
	// before another object is requested.
	Object(context.Context, git.Revision) (*Object, error)

	// ObjectQueue returns an ObjectQueue that can be used to batch multiple object requests.
	// Using the queue is more efficient than using `Object()` when requesting a bunch of
	// objects. The returned function must be executed after use of the ObjectQueue has
	// finished. Object Content and information can be requested from the queue but their
	// respective ordering must be maintained.
	ObjectQueue(context.Context) (ObjectQueue, func(), error)
}

// ObjectQueue allows for requesting and reading objects independently of each other. The number of
// RequestObject+RequestInfo and ReadObject+RequestInfo calls must match and their ordering must be
// maintained. ReadObject/ReadInfo must be executed after the object has been requested already.
// The order of objects returned by ReadObject/ReadInfo is the same as the order in
// which objects have been requested. Users of this interface must call `Flush()` after all requests
// have been queued up such that all requested objects will be readable.
type ObjectQueue interface {
	// RequestObject requests the given revision from git-cat-file(1).
	RequestObject(git.Revision) error
	// ReadObject reads an object which has previously been requested.
	ReadObject() (*Object, error)
	// RequestInfo requests the given revision from git-cat-file(1).
	RequestInfo(git.Revision) error
	// ReadInfo reads object info which has previously been requested.
	ReadInfo() (*ObjectInfo, error)
	// Flush flushes all queued requests and asks git-cat-file(1) to print all objects which
	// have been requested up to this point.
	Flush() error
}
