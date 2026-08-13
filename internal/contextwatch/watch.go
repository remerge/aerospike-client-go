package contextwatch

import (
	"context"
	"sync"
	"sync/atomic"
)

// Watch coordinates a context cancellation callback with resource reuse.
// Finish must be called before the protected resource is reused or released.
type Watch struct {
	ctx      context.Context
	stop     func() bool
	done     *sync.WaitGroup
	canceled *atomic.Bool
}

// Start registers interrupt to run when ctx is canceled.
// The returned Watch must be finished before the protected resource is reused or released.
func Start(ctx context.Context, interrupt func(force bool) bool) Watch {
	if ctx == nil || ctx.Done() == nil {
		return Watch{}
	}

	canceled := &atomic.Bool{}
	if ctx.Err() != nil {
		canceled.Store(true)
		interrupt(true)
		return Watch{ctx: ctx, canceled: canceled}
	}

	done := new(sync.WaitGroup)
	done.Add(1)
	stop := context.AfterFunc(ctx, func() {
		if interrupt(false) {
			canceled.Store(true)
		}
		defer done.Done()
	})
	return Watch{ctx: ctx, stop: stop, done: done, canceled: canceled}
}

// Finish stops a pending callback or waits for a running callback to finish.
// It returns the context error when the callback won the race.
func (watch Watch) Finish() error {
	if watch.canceled == nil {
		return nil
	}
	if watch.stop == nil {
		if watch.canceled.Load() {
			return watch.ctx.Err()
		}
		return nil
	}
	if watch.stop() {
		watch.done.Done()
		return nil
	}
	watch.done.Wait()
	if watch.canceled.Load() {
		return watch.ctx.Err()
	}
	return nil
}
