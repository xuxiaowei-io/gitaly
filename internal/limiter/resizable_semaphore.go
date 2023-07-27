package limiter

import (
	"context"
	"sync/atomic"
)

// resizableSemaphore struct models provides a way to bound concurrent access to resources. It allows a certain number
// of concurrent access to the resources. When the concurrency reaches the semaphore size, the callers are blocked until
// a resource is available again. The size of the semaphore can be resized freely in an atomic manner.
//
// Internally, a separate goroutine manages the semaphore's functionality, providing synchronization across all channel
// operations. Callers acquire the semaphore by pulling a "token" through an `acquireCh` channel via `acquire()`, and
// return them to an `releaseCh` channel via `release()`. This goroutine ensures that tokens are properly distributed
// from those channels, and also manages the semaphore's current length and size. It processes resize requests and
// handles try requests and responses, thus ensuring a smooth operation.
//
// Note: This struct is not intended to serve as a general-purpose data structure but is specifically designed for
// flexible concurrency control with resizable capacity.
type resizableSemaphore struct {
	// err stores the error of why the sempahore is stopped. Most of the case, it's due to context cancellation.
	err atomic.Pointer[error]
	// current represents the current concurrency access to the resources.
	current atomic.Int64
	// size is the maximum capacity of the semaphore. It represents the maximum concurrency that the resources can
	// be accessed of the time.
	size atomic.Int64
	// releaseCh is a channel used to return tokens back to the semaphore. When a caller returns a token, it sends a signal to this channel.
	releaseCh chan struct{}
	// acquireCh is a channel used for callers to acquire a token. When a token is available, a signal is sent to this channel.
	acquireCh chan bool
	// tryAcquireCh is a channel used to signal a try request. A try request is a non-blocking request to acquire a token.
	tryAcquireCh chan struct{}
	// tryAcquireRespCh is a channel used to respond to a try request. If a token is available, a nil error is sent; otherwise, an error is sent.
	tryAcquireRespCh chan bool
	// resizeCh is a channel used to request a resize of the semaphore's capacity. The requested new capacity is sent to this channel.
	resizeCh chan int64
	// stopCh is a channel that determines whether the semaphore is stopped.
	stopCh chan struct{}
}

// NewResizableSemaphore creates and starts a resizableSemaphore
func NewResizableSemaphore(ctx context.Context, capacity int64) *resizableSemaphore {
	s := &resizableSemaphore{
		stopCh:           make(chan struct{}),
		releaseCh:        make(chan struct{}),
		acquireCh:        make(chan bool),
		tryAcquireCh:     make(chan struct{}),
		tryAcquireRespCh: make(chan bool),
		resizeCh:         make(chan int64),
	}
	s.size.Store(capacity)
	go s.start(ctx)

	return s
}

// start kicks off a goroutine that maintains the states of the semaphore. It acts as an event loop that listens to
// all modifications.
func (q *resizableSemaphore) start(ctx context.Context) {
	for {
		if len := q.current.Load(); len < q.size.Load() {
			select {
			case <-q.releaseCh:
				q.current.Store(len - 1)
			case q.acquireCh <- true:
				q.current.Store(len + 1)
			case <-q.tryAcquireCh:
				q.current.Store(len + 1)
				q.tryAcquireRespCh <- true
			case newSize := <-q.resizeCh:
				// If the new capacity is greater than the prior one, the capacity grows without
				// the need to do anything. It allows more callers to acquire the token in the
				// right next iteration.
				// In contrast, when the new capacity is less than the prior one, the capacity
				// shrinks and the length becomes bigger than the new capacity. That's perfectly
				// fine. All existing callers continue, new callers can't acquire new token. The
				// length will be naturally adjusted below the capacity overtime.
				q.size.Store(newSize)
			case <-ctx.Done():
				q.stop(ctx.Err())
				return
			}
		} else {
			select {
			case <-q.releaseCh:
				q.current.Store(len - 1)
			case <-q.tryAcquireCh:
				q.tryAcquireRespCh <- false
			case newSize := <-q.resizeCh:
				// Simiarly to the above case, overriding the capacity is enough.
				q.size.Store(newSize)
			case <-ctx.Done():
				q.stop(ctx.Err())
				return
			}
		}
	}
}

func (q *resizableSemaphore) stop(err error) {
	q.current.Store(0)
	q.err.Store(&err)

	close(q.stopCh)
	// The only exposed channel. After this channel is closed, caller receives `false` when trying to pull from this
	// channel. Other state-modifying channels stay instact but TryAcquire(), Release(), and Resize() returns
	// immediately.
	close(q.acquireCh)
}

// stores the error of why the sempahore is stopped. Most of the case, it's due to context cancellation.
func (q *resizableSemaphore) Err() error {
	return *q.err.Load()
}

// Acquire returns a channel that allows the caller to acquire the semaphore. The caller is blocked until there
// is an available resource. It is not safe to use with a `switch` statement having `default` statement. In such
// use cases, use TryAcquire() instead.
func (q *resizableSemaphore) Acquire() <-chan bool {
	return q.acquireCh
}

// TryAcquire acquires the semaphore without blocking. On success, returns true. On failure, returns false and leaves
// the semaphore unchanged.
func (q *resizableSemaphore) TryAcquire() bool {
	select {
	case q.tryAcquireCh <- struct{}{}:
		return <-q.tryAcquireRespCh
	case <-q.stopCh:
		return false
	}
}

// Release releases the semaphore by pushing the token back. If the semaphore stops, this function returns immediately.
func (q *resizableSemaphore) Release() {
	select {
	case q.releaseCh <- struct{}{}:
	case <-q.stopCh:
		// No op, return immediately.
	}
}

// Current returns the amount of current concurrent access to the semaphore.
func (q *resizableSemaphore) Current() int64 {
	return q.current.Load()
}

// Resize modifies the size of the semaphore. If the semaphore stops, this function returns immediately.
func (q *resizableSemaphore) Resize(newSize int64) {
	select {
	case q.resizeCh <- newSize:
	case <-q.stopCh:
		// This update is redundant, but it's a nice gesture that the state of semaphore is up-to-date.
		q.size.Store(newSize)
	}
}
