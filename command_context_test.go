package aerospike

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCommandContextWatchInterruptsBlockedIO(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cmd := baseCommand{ctx: ctx}
	connection := &Connection{conn: clientConn}
	watch := cmd.watchContext(connection)

	readDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Read(make([]byte, 1))
		readDone <- err
	}()

	cancel()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("blocked read was not interrupted")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read did not return after cancellation")
	}

	if err := watch.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if !connection.interrupted.Load() {
		t.Fatal("connection was not marked as interrupted")
	}
}

func TestCommandContextWatchStopPreventsLateConnectionInterruption(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cmd := baseCommand{ctx: ctx}
	connection := &Connection{conn: clientConn}
	watch := cmd.watchContext(connection)

	if err := watch.Finish(); err != nil {
		t.Fatalf("unexpected context watch error: %v", err)
	}
	cancel()

	writeDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Write([]byte{42})
		writeDone <- err
	}()

	if err := clientConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := clientConn.Read(buffer); err != nil {
		t.Fatalf("connection was interrupted after watch stopped: %v", err)
	}
	if buffer[0] != 42 {
		t.Fatalf("unexpected byte: %d", buffer[0])
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if connection.interrupted.Load() {
		t.Fatal("stopped callback marked the connection as interrupted")
	}
}

func TestConnectionRefreshClearsInterruptedState(t *testing.T) {
	connection := &Connection{bufferAdjustDeadline: time.Now().Add(time.Hour)}
	connection.interrupted.Store(true)
	connection.refresh()
	if connection.interrupted.Load() {
		t.Fatal("refresh did not clear interrupted state")
	}
}
