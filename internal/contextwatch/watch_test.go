package contextwatch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestCancelRunsInterruptBeforeFinishReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	watch := Start(ctx, func(force bool) bool {
		interrupted.Store(true)
		return true
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
	var forced atomic.Bool
	watch := Start(ctx, func(force bool) bool {
		forced.Store(force)
		interrupted.Add(1)
		return true
	})

	if interrupted.Load() != 1 {
		t.Fatalf("expected one interrupt, got %d", interrupted.Load())
	}
	if !forced.Load() {
		t.Fatal("expected forced interrupt for pre-canceled context")
	}
	if err := watch.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestFinishPreventsLateInterrupt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	watch := Start(ctx, func(force bool) bool {
		interrupted.Store(true)
		return true
	})

	if err := watch.Finish(); err != nil {
		t.Fatalf("unexpected finish error: %v", err)
	}
	cancel()
	if interrupted.Load() {
		t.Fatal("callback ran after finish stopped it")
	}
}

func TestFinishReturnsNilWhenLateCancelFindsNoActiveIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callbackRan := make(chan struct{})
	watch := Start(ctx, func(force bool) bool {
		close(callbackRan)
		return false
	})

	cancel()
	<-callbackRan

	if err := watch.Finish(); err != nil {
		t.Fatalf("expected successful finish, got %v", err)
	}
}

func TestCancelFinishRaceAlwaysCoordinates(t *testing.T) {
	for range 10_000 {
		ctx, cancel := context.WithCancel(context.Background())
		var interrupted atomic.Bool
		watch := Start(ctx, func(force bool) bool {
			interrupted.Store(true)
			return true
		})

		go cancel()
		err := watch.Finish()
		if err != nil && !interrupted.Load() {
			t.Fatal("finish reported cancellation before interrupt completed")
		}
	}
}
