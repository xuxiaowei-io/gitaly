package limiter

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func TestResizableSemaphore_New(t *testing.T) {
	t.Parallel()

	semaphore := NewResizableSemaphore(testhelper.Context(t), 5)
	require.Equal(t, int64(0), semaphore.Current())
}

func TestResizableSemaphore_Stopped(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	semaphore := NewResizableSemaphore(ctx, 5)

	// Try to acquire a token of the empty sempahore
	require.True(t, semaphore.TryAcquire())
	semaphore.Release()

	// 5 goroutines acquired semaphore
	beforeCallRelease := make(chan struct{})
	var acquireWg1, releaseWg1 sync.WaitGroup
	for i := 0; i < 5; i++ {
		acquireWg1.Add(1)
		releaseWg1.Add(1)
		go func() {
			require.True(t, <-semaphore.Acquire())
			acquireWg1.Done()

			<-beforeCallRelease

			semaphore.Release()
			releaseWg1.Done()
		}()
	}
	acquireWg1.Wait()

	//  Another 5 waits for sempahore
	var acquireWg2 sync.WaitGroup
	for i := 0; i < 5; i++ {
		acquireWg2.Add(1)
		go func() {
			// This goroutine is block until the context is cancel, which returns false
			require.False(t, <-semaphore.Acquire())
			acquireWg2.Done()
		}()
	}

	// Try to acquire a token of the full semaphore
	require.False(t, semaphore.TryAcquire())

	// Cancel the context
	cancel()
	acquireWg2.Wait()

	// The first 5 goroutines can call Release() even if the semaphore stopped
	close(beforeCallRelease)
	releaseWg1.Wait()

	// The last 5 goroutines exits immediately, Acquire() returns false
	acquireWg2.Wait()

	require.False(t, semaphore.TryAcquire())
	require.Equal(t, int64(0), semaphore.Current())
	require.Equal(t, ctx.Err(), semaphore.Err())
}

func TestResizableSemaphore_Acquire(t *testing.T) {
	t.Parallel()

	t.Run("acquire less than the capacity", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 5)

		waitBeforeRelease, waitRelease := acquireSemaphore(t, semaphore, 4)
		require.NotNil(t, <-semaphore.Acquire())

		close(waitBeforeRelease)
		waitRelease()
	})

	t.Run("acquire more than the capacity", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 5)

		waitBeforeRelease, waitRelease := acquireSemaphore(t, semaphore, 5)
		require.False(t, semaphore.TryAcquire())

		close(waitBeforeRelease)
		waitRelease()
	})

	t.Run("semaphore is full then available again", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 5)

		waitChan := make(chan bool)
		_, _ = acquireSemaphore(t, semaphore, 5)

		go func() {
			waitChan <- <-semaphore.Acquire()
		}()

		// The semaphore is full now
		require.False(t, semaphore.TryAcquire())

		// Release one token
		semaphore.Release()
		// The waiting channel is unlocked
		require.True(t, <-waitChan)

		// Release another token
		semaphore.Release()
		// Now TryAcquire can pull out a token
		require.True(t, semaphore.TryAcquire())
	})

	t.Run("the semaphore is resized up when empty", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 5)
		semaphore.Resize(10)

		waitBeforeRelease, waitRelease := acquireSemaphore(t, semaphore, 9)
		require.NotNil(t, <-semaphore.Acquire())

		close(waitBeforeRelease)
		waitRelease()
	})

	t.Run("the semaphore is resized up when not empty", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 7)

		waitBeforeRelease1, waitRelease1 := acquireSemaphore(t, semaphore, 5)
		semaphore.Resize(15)
		waitBeforeRelease2, waitRelease2 := acquireSemaphore(t, semaphore, 5)

		require.NotNil(t, <-semaphore.Acquire())

		close(waitBeforeRelease1)
		close(waitBeforeRelease2)
		waitRelease1()
		waitRelease2()
	})

	t.Run("the semaphore is resized up when full", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 5)

		waitBeforeRelease1, waitRelease1 := acquireSemaphore(t, semaphore, 5)
		require.False(t, semaphore.TryAcquire())

		semaphore.Resize(10)

		waitBeforeRelease2, waitRelease2 := acquireSemaphore(t, semaphore, 5)
		require.False(t, semaphore.TryAcquire())

		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				<-semaphore.Acquire()
				wg.Done()
				semaphore.Release()
			}()
		}

		semaphore.Resize(15)
		wg.Wait()

		close(waitBeforeRelease1)
		close(waitBeforeRelease2)
		waitRelease1()
		waitRelease2()
	})

	t.Run("the semaphore is resized down when empty", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 10)
		semaphore.Resize(5)

		waitBeforeRelease, waitRelease := acquireSemaphore(t, semaphore, 4)
		require.NotNil(t, <-semaphore.Acquire())

		close(waitBeforeRelease)
		waitRelease()
	})

	t.Run("the semaphore is resized down when not empty", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 20)

		waitBeforeRelease1, waitRelease1 := acquireSemaphore(t, semaphore, 5)
		semaphore.Resize(15)
		waitBeforeRelease2, waitRelease2 := acquireSemaphore(t, semaphore, 5)

		require.NotNil(t, <-semaphore.Acquire())

		close(waitBeforeRelease1)
		close(waitBeforeRelease2)
		waitRelease1()
		waitRelease2()
	})

	t.Run("the semaphore is resized down lower than the current length", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 10)

		waitBeforeRelease1, waitRelease1 := acquireSemaphore(t, semaphore, 5)

		semaphore.Resize(3)

		require.False(t, semaphore.TryAcquire())
		close(waitBeforeRelease1)
		waitRelease1()

		waitBeforeRelease2, waitRelease2 := acquireSemaphore(t, semaphore, 3)
		require.False(t, semaphore.TryAcquire())

		close(waitBeforeRelease2)
		waitRelease2()
	})

	t.Run("the semaphore is resized down when full", func(t *testing.T) {
		semaphore := NewResizableSemaphore(testhelper.Context(t), 10)

		waitBeforeRelease1, waitRelease1 := acquireSemaphore(t, semaphore, 10)

		semaphore.Resize(5)

		require.False(t, semaphore.TryAcquire())
		close(waitBeforeRelease1)
		waitRelease1()

		waitBeforeRelease2, waitRelease2 := acquireSemaphore(t, semaphore, 5)
		require.False(t, semaphore.TryAcquire())

		close(waitBeforeRelease2)
		waitRelease2()
	})
}

func acquireSemaphore(t *testing.T, semaphore *resizableSemaphore, n int) (chan struct{}, func()) {
	var acquireWg, releaseWg sync.WaitGroup
	waitBeforeRelease := make(chan struct{})

	for i := 0; i < n; i++ {
		acquireWg.Add(1)
		releaseWg.Add(1)
		go func() {
			require.True(t, <-semaphore.Acquire())
			acquireWg.Done()

			<-waitBeforeRelease
			semaphore.Release()
			releaseWg.Done()
		}()
	}
	acquireWg.Wait()

	return waitBeforeRelease, releaseWg.Wait
}
