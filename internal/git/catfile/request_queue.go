package catfile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

const (
	// flushCommand is the command we send to git-cat-file(1) to cause it to flush its stdout.
	// Note that this is a hack: git-cat-file(1) doesn't really support flushing, but it will
	// flush whenever it encounters an object it doesn't know. The flush command we use is thus
	// chosen such that it cannot ever refer to a valid object: refs may not contain whitespace,
	// so this command cannot refer to a ref. Adding "FLUSH" is just for the sake of making it
	// easier to spot what's going on in case we ever mistakenly see this output in the wild.
	flushCommand = "\tFLUSH\t"
)

type requestQueue struct {
	// outstandingRequests is the number of requests which have been queued up. Gets incremented
	// on request, and decremented when starting to read an object (not when that object has
	// been fully consumed).
	//
	// We list the atomic fields first to ensure they are 64-bit and 32-bit aligned:
	// https://pkg.go.dev/sync/atomic#pkg-note-BUG
	outstandingRequests int64

	// closed indicates whether the queue is closed for additional requests.
	closed int32

	// isReadingObject indicates whether the queue is currently in `ReadObject()` to
	// read in a new object's header.
	isReadingObject int32

	// isObjectQueue is set to `true` when this is a request queue which can be used for reading
	// objects. If set to `false`, then this can only be used to read object info.
	isObjectQueue bool

	stdout *bufio.Reader
	stdin  *bufio.Writer

	// currentObject is the currently read object.
	currentObject     *Object
	currentObjectLock sync.Mutex

	// trace is the current tracing span.
	trace *trace
}

// isDirty returns true either if there are outstanding requests for objects or if the current
// object hasn't yet been fully consumed.
func (q *requestQueue) isDirty() bool {
	// We need to take this lock early. Like this we can ensure that `ReadObject()`
	// cannot have it at the same time while `isReadingObject == 0`.
	q.currentObjectLock.Lock()
	defer q.currentObjectLock.Unlock()

	// We must check for the current object first: we cannot queue another object due to the
	// object lock, but we may queue another request while checking for dirtiness.
	if q.currentObject != nil && q.currentObject.isDirty() {
		return true
	}

	if atomic.LoadInt32(&q.isReadingObject) != 0 {
		return true
	}

	if atomic.LoadInt64(&q.outstandingRequests) != 0 {
		return true
	}

	return false
}

func (q *requestQueue) isClosed() bool {
	return atomic.LoadInt32(&q.closed) == 1
}

func (q *requestQueue) close() {
	if atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		q.currentObjectLock.Lock()
		defer q.currentObjectLock.Unlock()

		if q.currentObject != nil {
			q.currentObject.close()
		}
	}
}

func (q *requestQueue) RequestRevision(revision git.Revision) error {
	if q.isClosed() {
		return fmt.Errorf("cannot request revision: %w", os.ErrClosed)
	}

	atomic.AddInt64(&q.outstandingRequests, 1)

	if _, err := q.stdin.WriteString(revision.String()); err != nil {
		atomic.AddInt64(&q.outstandingRequests, -1)
		return fmt.Errorf("writing object request: %w", err)
	}

	if err := q.stdin.WriteByte('\n'); err != nil {
		atomic.AddInt64(&q.outstandingRequests, -1)
		return fmt.Errorf("terminating object request: %w", err)
	}

	return nil
}

func (q *requestQueue) Flush() error {
	if q.isClosed() {
		return fmt.Errorf("cannot flush: %w", os.ErrClosed)
	}

	if _, err := q.stdin.WriteString(flushCommand); err != nil {
		return fmt.Errorf("writing flush command: %w", err)
	}

	if err := q.stdin.WriteByte('\n'); err != nil {
		return fmt.Errorf("terminating flush command: %w", err)
	}

	if err := q.stdin.Flush(); err != nil {
		return fmt.Errorf("flushing: %w", err)
	}

	return nil
}

func (q *requestQueue) ReadObject() (*Object, error) {
	if !q.isObjectQueue {
		panic("object queue used to read object info")
	}

	// We need to ensure that only a single call to `ReadObject()` can happen at the
	// same point in time. While the easy solution would be to just lock
	// `currentObjectLock()`, that would mean we hold the lock while performing I/O
	// operations. That's a no-go though because the command may stall, and would
	// deadlock with `isDirty()` and `close()`.
	//
	// Note that this must happen before we read the current object: otherwise, a
	// concurrent caller might've already swapped it out under our feet.
	if !atomic.CompareAndSwapInt32(&q.isReadingObject, 0, 1) {
		return nil, fmt.Errorf("concurrent read of the object")
	}
	defer func() {
		atomic.StoreInt32(&q.isReadingObject, 0)
	}()

	// We know no other user can change `currentObject` right now anymore, but we still
	// need to take `currentObjectLock()` given that concurrent callers may try to access the
	// object.
	q.currentObjectLock.Lock()
	currentObject := q.currentObject
	q.currentObjectLock.Unlock()

	if currentObject != nil {
		// If the current object is still dirty, then we must not try to read a new object.
		if currentObject.isDirty() {
			return nil, fmt.Errorf("current object has not been fully read")
		}

		currentObject.close()

		q.currentObjectLock.Lock()
		q.currentObject = nil
		q.currentObjectLock.Unlock()

		// If we have already read an object before, then we must consume the trailing
		// newline after the object's data.
		if _, err := q.stdout.ReadByte(); err != nil {
			return nil, err
		}
	}

	objectInfo, err := q.readInfo()
	if err != nil {
		return nil, err
	}
	q.trace.recordRequest(objectInfo.Type)

	// Synchronize with readers of the object.
	q.currentObjectLock.Lock()
	defer q.currentObjectLock.Unlock()

	q.currentObject = &Object{
		ObjectInfo: *objectInfo,
		dataReader: io.LimitedReader{
			R: q.stdout,
			N: objectInfo.Size,
		},
		bytesRemaining: objectInfo.Size,
	}

	return q.currentObject, nil
}

func (q *requestQueue) ReadInfo() (*ObjectInfo, error) {
	if q.isObjectQueue {
		panic("object queue used to read object info")
	}

	objectInfo, err := q.readInfo()
	if err != nil {
		return nil, err
	}
	q.trace.recordRequest("info")

	return objectInfo, nil
}

func (q *requestQueue) readInfo() (*ObjectInfo, error) {
	if q.isClosed() {
		return nil, fmt.Errorf("cannot read object info: %w", os.ErrClosed)
	}

	// We first need to determine wether there are any queued requests at all. If not, then we
	// cannot read anything.
	queuedRequests := atomic.LoadInt64(&q.outstandingRequests)
	if queuedRequests == 0 {
		return nil, fmt.Errorf("no outstanding request")
	}

	// We cannot check whether `outstandingRequests` is strictly smaller than before given
	// that there may be a concurrent caller who requests additional objects, which would in
	// turn increment the counter. But we can at least verify that it's not smaller than what we
	// expect to give us the chance to detect concurrent reads.
	if atomic.AddInt64(&q.outstandingRequests, -1) < queuedRequests-1 {
		return nil, fmt.Errorf("concurrent read on request queue")
	}

	return ParseObjectInfo(q.stdout)
}
