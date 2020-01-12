package ctxwatch

import (
	"context"
	"sync/atomic"
)

// ContextWatcher watches a context and performs an action when the context is canceled. It can watch one context at a
// time.
type ContextWatcher struct {
	onCancel             func()
	onUnwatchAfterCancel func()
	canceled             chan bool
	watching             uint32
}

// NewContextWatcher returns a ContextWatcher. onCancel will be called when a watched context is canceled.
// OnUnwatchAfterCancel will be called when Unwatch is called and the watched context had already been canceled and
// onCancel called.
func NewContextWatcher(onCancel func(), onUnwatchAfterCancel func()) *ContextWatcher {
	cw := &ContextWatcher{
		onCancel:             onCancel,
		onUnwatchAfterCancel: onUnwatchAfterCancel,
		canceled:             make(chan bool),
	}

	return cw
}

// Watch starts watching ctx. If ctx is canceled then the onCancel function passed to NewContextWatcher will be called.
func (cw *ContextWatcher) Watch(ctx context.Context) {
	shouldWatch := uint32(1)
	if ctx.Done() == nil {
		shouldWatch = 0
	}

	if swapped := atomic.CompareAndSwapUint32(&cw.watching, 0, shouldWatch); !swapped {
		panic("Watch already in progress")
	}

	if shouldWatch == 1 {
		go cw.watch(ctx)
	}
}

func (cw *ContextWatcher) watch(ctx context.Context) {
	select {
	case <-ctx.Done():
		watching := atomic.LoadUint32(&cw.watching) == 1
		if watching {
			cw.onCancel()
		}
		cw.canceled <- watching

	case cw.canceled <- false:
	}
}

// Unwatch stops watching the previously watched context. If the onCancel function passed to NewContextWatcher was
// called then onUnwatchAfterCancel will also be called.
func (cw *ContextWatcher) Unwatch() {
	watching := atomic.CompareAndSwapUint32(&cw.watching, 1, 0)
	if !watching {
		return
	}

	canceled := <-cw.canceled
	if canceled {
		cw.onUnwatchAfterCancel()
	}
}
