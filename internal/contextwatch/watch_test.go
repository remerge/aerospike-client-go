package contextwatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCancelRunsInterruptBeforeFinishReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	watch := Start(ctx, func() {
		interrupted.Store(true)
	})

	cancel()
	if err := watch.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if !interrupted.Load() {
		t.Fatal("finish returned before interrupt completed")
	}
}

func TestStartAlreadyCanceledContextInterruptsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var interrupted atomic.Int32
	watch := Start(ctx, func() {
		interrupted.Add(1)
	})

	if interrupted.Load() != 1 {
		t.Fatalf("expected one interrupt, got %d", interrupted.Load())
	}
	if err := watch.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestFinishPreventsLateInterrupt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	watch := Start(ctx, func() {
		interrupted.Store(true)
	})

	if err := watch.Finish(); err != nil {
		t.Fatalf("unexpected finish error: %v", err)
	}
	cancel()
	if interrupted.Load() {
		t.Fatal("callback ran after finish stopped it")
	}
}

func TestFinishReturnsNilWhenStopWinsAfterLateCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := new(sync.WaitGroup)
	done.Add(1)
	watch := Watch{
		ctx:         ctx,
		stop:        func() bool { return true },
		done:        done,
		interrupted: &atomic.Bool{},
	}

	if err := watch.Finish(); err != nil {
		t.Fatalf("expected successful finish, got %v", err)
	}
}

func TestCancelFinishRaceAlwaysCoordinates(t *testing.T) {
	for range 10_000 {
		ctx, cancel := context.WithCancel(context.Background())
		var interrupted atomic.Bool
		watch := Start(ctx, func() {
			interrupted.Store(true)
		})

		go cancel()
		err := watch.Finish()
		if err != nil && !interrupted.Load() {
			t.Fatal("finish reported cancellation before interrupt completed")
		}
	}
}
