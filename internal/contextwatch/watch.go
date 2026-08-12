package contextwatch

import (
	"context"
	"sync"
)

// Watch coordinates a context cancellation callback with resource reuse.
// Finish must be called before the protected resource is reused or released.
type Watch struct {
	ctx  context.Context
	stop func() bool
	done *sync.WaitGroup
}

func Start(ctx context.Context, interrupt func()) Watch {
	if ctx == nil || ctx.Done() == nil {
		return Watch{}
	}

	done := new(sync.WaitGroup)
	done.Add(1)
	stop := context.AfterFunc(ctx, func() {
		defer done.Done()
		interrupt()
	})
	return Watch{ctx: ctx, stop: stop, done: done}
}

// Finish stops a pending callback or waits for a running callback to finish.
// It returns the context error when the callback won the race.
func (watch Watch) Finish() error {
	if watch.stop == nil {
		return nil
	}
	if watch.stop() {
		watch.done.Done()
		return nil
	}
	watch.done.Wait()
	return watch.ctx.Err()
}
