package contextwatch

import (
	"context"
	"sync"
	"sync/atomic"
)

// Watch coordinates a context cancellation callback with resource reuse.
// Finish must be called before the protected resource is reused or released.
type Watch struct {
	ctx         context.Context
	stop        func() bool
	done        *sync.WaitGroup
	interrupted *atomic.Bool
}

// Start registers interrupt to run when ctx is canceled.
// The returned Watch must be finished before the protected resource is reused or released.
func Start(ctx context.Context, interrupt func()) Watch {
	if ctx == nil || ctx.Done() == nil {
		return Watch{}
	}

	interrupted := &atomic.Bool{}
	if ctx.Err() != nil {
		interrupted.Store(true)
		interrupt()
		return Watch{ctx: ctx, interrupted: interrupted}
	}

	done := new(sync.WaitGroup)
	done.Add(1)
	stop := context.AfterFunc(ctx, func() {
		interrupted.Store(true)
		defer done.Done()
		interrupt()
	})
	return Watch{ctx: ctx, stop: stop, done: done, interrupted: interrupted}
}

// Finish stops a pending callback or waits for a running callback to finish.
// It returns the context error when the callback won the race.
func (watch Watch) Finish() error {
	if watch.interrupted == nil {
		return nil
	}
	if watch.stop == nil {
		if watch.interrupted.Load() {
			return watch.ctx.Err()
		}
		return nil
	}
	if watch.stop() {
		watch.done.Done()
		return nil
	}
	watch.done.Wait()
	if watch.interrupted.Load() {
		return watch.ctx.Err()
	}
	return nil
}
