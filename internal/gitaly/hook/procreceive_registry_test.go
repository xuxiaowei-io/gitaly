package hook

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"golang.org/x/sync/errgroup"
)

func TestProcReceiveRegistry(t *testing.T) {
	t.Parallel()

	newInvocation := func(id storage.TransactionID) *procReceiveHookInvocation {
		return newProcReceiveHookInvocation(false, id, nil, nil, nil, nil)
	}

	t.Run("invocation added and received", func(t *testing.T) {
		t.Parallel()

		registry := NewProcReceiveRegistry()
		invocation := newInvocation(1)
		registry.set(invocation)
		receivedInvocation := registry.Get(1)
		require.Equal(t, invocation, receivedInvocation)
	})

	t.Run("invocation added after receiver", func(t *testing.T) {
		t.Parallel()

		registry := NewProcReceiveRegistry()
		invocation := newInvocation(1)

		go func() {
			registry.set(invocation)
		}()

		receivedInvocation := registry.Get(1)
		require.Equal(t, invocation, receivedInvocation)
	})

	t.Run("invocation not received", func(t *testing.T) {
		t.Parallel()

		registry := NewProcReceiveRegistry()
		invocation := newInvocation(1)

		// Shouldn't block and should finish the test.
		registry.set(invocation)
	})

	t.Run("invocation added twice", func(t *testing.T) {
		t.Parallel()

		registry := NewProcReceiveRegistry()
		invocation := newInvocation(1)

		// set() is idempotent, so makes no difference.
		registry.set(invocation)
		registry.set(invocation)
	})

	t.Run("multiple invocations", func(t *testing.T) {
		t.Parallel()

		registry := NewProcReceiveRegistry()

		invocation1 := newInvocation(1)
		invocation2 := newInvocation(2)
		invocation3 := newInvocation(3)

		group, _ := errgroup.WithContext(context.Background())

		group.Go(func() error {
			receivedInvocation := registry.Get(1)
			if receivedInvocation != invocation1 {
				return fmt.Errorf("invalid invocation: %d", 1)
			}
			return nil
		})

		group.Go(func() error {
			receivedInvocation := registry.Get(2)
			if receivedInvocation != invocation2 {
				return fmt.Errorf("invalid invocation: %d", 2)
			}
			return nil
		})

		group.Go(func() error {
			receivedInvocation := registry.Get(3)
			if receivedInvocation != invocation3 {
				return fmt.Errorf("invalid invocation: %d", 3)
			}
			return nil
		})

		group.Go(func() error {
			registry.set(invocation1)
			registry.set(invocation2)
			registry.set(invocation3)

			return nil
		})

		require.NoError(t, group.Wait())
	})
}
