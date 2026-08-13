package aerospike

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"iter"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
)

func TestCommandContextWatchInterruptsBlockedIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := baseCommand{ctx: ctx}
	netConn := newBlockingNetConn()
	connection := &Connection{
		conn:                 netConn,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	watch := cmd.watchContext(connection)

	readDone := make(chan Error, 1)
	go func() {
		_, err := connection.Read(make([]byte, 1), 1)
		readDone <- err
	}()

	select {
	case <-netConn.readStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked read did not start")
	}

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
	connection.socketState.Store(socketStateInterrupted)
	connection.refresh()
	if connection.interrupted.Load() {
		t.Fatal("refresh did not clear interrupted state")
	}
	if state := connection.socketState.Load(); state != socketStateIdle {
		t.Fatalf("expected idle socket state, got %d", state)
	}
}

func TestConnectionInterruptIOOnlyWhileSocketIOIsActive(t *testing.T) {
	connection := &Connection{}
	if err := connection.beginSocketIO(); err != nil {
		t.Fatalf("beginSocketIO failed: %v", err)
	}
	if !connection.interruptIO(false) {
		t.Fatal("expected active socket I/O to be interrupted")
	}
	if connection.completeSocketIO() {
		t.Fatal("completion should lose once interruption wins")
	}
	if !connection.interrupted.Load() {
		t.Fatal("active socket interruption did not poison the connection")
	}
}

func TestConnectionInterruptIOAfterSocketIOCompletesIsIgnored(t *testing.T) {
	connection := &Connection{}
	if err := connection.beginSocketIO(); err != nil {
		t.Fatalf("beginSocketIO failed: %v", err)
	}
	if !connection.completeSocketIO() {
		t.Fatal("completion should win before a late interruption")
	}
	if connection.interruptIO(false) {
		t.Fatal("late interruption should not win after socket I/O completed")
	}
	if connection.interrupted.Load() {
		t.Fatal("late interruption poisoned a healthy connection")
	}
}

type stubAddr string

func (a stubAddr) Network() string { return string(a) }
func (a stubAddr) String() string  { return string(a) }

type stubNetConn struct {
	closed        bool
	deadlineCalls int
	lastDeadline  time.Time
	writeCalls    int
}

type blockingNetConn struct {
	readStarted chan struct{}
	interrupted chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	closed      atomic.Bool
}

type finalReadNetConn struct {
	readStarted chan struct{}
	interrupted chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	closed      atomic.Bool
}

func newBlockingNetConn() *blockingNetConn {
	return &blockingNetConn{
		readStarted: make(chan struct{}),
		interrupted: make(chan struct{}),
	}
}

func newFinalReadNetConn() *finalReadNetConn {
	return &finalReadNetConn{
		readStarted: make(chan struct{}),
		interrupted: make(chan struct{}),
	}
}

func (c *blockingNetConn) Read(_ []byte) (int, error) {
	c.startOnce.Do(func() { close(c.readStarted) })
	<-c.interrupted
	return 0, os.ErrDeadlineExceeded
}

func (*blockingNetConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *blockingNetConn) Close() error {
	c.closed.Store(true)
	c.stopOnce.Do(func() { close(c.interrupted) })
	return nil
}
func (*blockingNetConn) LocalAddr() net.Addr  { return stubAddr("local") }
func (*blockingNetConn) RemoteAddr() net.Addr { return stubAddr("remote") }
func (c *blockingNetConn) SetDeadline(deadline time.Time) error {
	if !deadline.After(time.Now()) {
		c.stopOnce.Do(func() { close(c.interrupted) })
	}
	return nil
}
func (c *blockingNetConn) SetReadDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}
func (c *blockingNetConn) SetWriteDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}

func (c *finalReadNetConn) Read(b []byte) (int, error) {
	c.startOnce.Do(func() { close(c.readStarted) })
	<-c.interrupted
	b[0] = 42
	return 1, os.ErrDeadlineExceeded
}

func (*finalReadNetConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *finalReadNetConn) Close() error {
	c.closed.Store(true)
	c.stopOnce.Do(func() { close(c.interrupted) })
	return nil
}
func (*finalReadNetConn) LocalAddr() net.Addr  { return stubAddr("local") }
func (*finalReadNetConn) RemoteAddr() net.Addr { return stubAddr("remote") }
func (c *finalReadNetConn) SetDeadline(deadline time.Time) error {
	if !deadline.After(time.Now()) {
		c.stopOnce.Do(func() { close(c.interrupted) })
	}
	return nil
}
func (c *finalReadNetConn) SetReadDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}
func (c *finalReadNetConn) SetWriteDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}

func (c *stubNetConn) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (c *stubNetConn) Write(b []byte) (int, error) { c.writeCalls++; return len(b), nil }
func (c *stubNetConn) Close() error {
	c.closed = true
	return nil
}
func (c *stubNetConn) LocalAddr() net.Addr  { return stubAddr("local") }
func (c *stubNetConn) RemoteAddr() net.Addr { return stubAddr("remote") }
func (c *stubNetConn) SetDeadline(t time.Time) error {
	c.deadlineCalls++
	c.lastDeadline = t
	return nil
}
func (c *stubNetConn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *stubNetConn) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

type deadlineRaceContext struct {
	deadline time.Time
	done     chan struct{}
}

func (ctx *deadlineRaceContext) Deadline() (time.Time, bool) { return ctx.deadline, true }
func (ctx *deadlineRaceContext) Done() <-chan struct{}       { return ctx.done }
func (ctx *deadlineRaceContext) Err() error                  { return nil }
func (ctx *deadlineRaceContext) Value(key any) any           { return nil }

type commandExecutionStub struct {
	baseCommand
	policy          *BasePolicy
	acquiredConn    *Connection
	getConnHook     func(*commandExecutionStub) (*Connection, Error)
	writeBufferHook func(*commandExecutionStub) Error
	parseResultHook func(*commandExecutionStub) Error
	putConnCalls    int
	returnedConn    *Connection
}

func newCommandExecutionStub(ctx context.Context, conn *Connection) *commandExecutionStub {
	clientPolicy := &atomic.Pointer[ClientPolicy]{}
	clientPolicy.Store(NewClientPolicy())
	node := &Node{
		cluster:             &Cluster{clientPolicy: clientPolicy, maxErrorCount: *iatomic.NewInt(1)},
		name:                "test-node",
		host:                &Host{Name: "127.0.0.1", Port: 3000},
		stats:               *newNodeStats(DefaultMetricsPolicy()),
		partitionGeneration: *iatomic.NewInt(0),
		active:              *iatomic.NewBool(true),
		errorCount:          *iatomic.NewInt(0),
		maxErrorCount:       *iatomic.NewInt(1),
	}
	conn.node = node
	return &commandExecutionStub{
		baseCommand:  baseCommand{ctx: ctx},
		policy:       &BasePolicy{MaxRetries: 0},
		acquiredConn: conn,
	}
}

func (cmd *commandExecutionStub) getPolicy(ifc command) Policy { return cmd.policy }
func (cmd *commandExecutionStub) getNode(ifc command) (*Node, Error) {
	cmd.node = cmd.acquiredConn.node
	return cmd.node, nil
}
func (cmd *commandExecutionStub) getConnection(policy Policy) (*Connection, Error) {
	if cmd.getConnHook != nil {
		return cmd.getConnHook(cmd)
	}
	return cmd.acquiredConn, nil
}
func (cmd *commandExecutionStub) putConnection(conn *Connection) {
	cmd.putConnCalls++
	cmd.returnedConn = conn
	conn.refresh()
}
func (cmd *commandExecutionStub) writeBuffer(ifc command) Error {
	if cmd.writeBufferHook != nil {
		return cmd.writeBufferHook(cmd)
	}
	cmd.dataOffset = 30
	return nil
}
func (cmd *commandExecutionStub) parseResult(ifc command, conn *Connection) Error {
	if cmd.parseResultHook != nil {
		return cmd.parseResultHook(cmd)
	}
	return nil
}
func (cmd *commandExecutionStub) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	return false, nil
}
func (cmd *commandExecutionStub) prepareRetry(ifc command, isTimeout bool) bool { return false }
func (cmd *commandExecutionStub) commandType() commandType                      { return ttGet }
func (cmd *commandExecutionStub) isRead() bool                                  { return true }
func (cmd *commandExecutionStub) onInDoubt()                                    {}
func (cmd *commandExecutionStub) execute(ifc command) Error                     { return cmd.baseCommand.execute(ifc) }
func (cmd *commandExecutionStub) executeIter(ifc command, iter int) Error {
	return cmd.baseCommand.executeIter(ifc, iter)
}
func (cmd *commandExecutionStub) executeAt(ifc command, policy *BasePolicy, deadline time.Time, iterations int) Error {
	return cmd.baseCommand.executeAt(ifc, policy, deadline, iterations)
}
func (cmd *commandExecutionStub) canPutConnBack() bool                     { return true }
func (cmd *commandExecutionStub) getNamespaces() iter.Seq2[string, uint64] { return nil }
func (cmd *commandExecutionStub) getNamespace() *string                    { return nil }
func (cmd *commandExecutionStub) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
}
func (cmd *commandExecutionStub) Execute() Error { return cmd.execute(cmd) }

func newStubConnection() (*Connection, *stubNetConn) {
	netConn := &stubNetConn{}
	buffer := make([]byte, 64)
	conn := &Connection{
		conn:                 netConn,
		dataBuffer:           buffer,
		origDataBuffer:       buffer,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	return conn, netConn
}

func TestExecuteAtReturnsCleanConnectionOnCancellationBeforeSend(t *testing.T) {
	conn, netConn := newStubConnection()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.getConnHook = func(*commandExecutionStub) (*Connection, Error) {
		cancel()
		return conn, nil
	}

	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if cmd.putConnCalls != 1 {
		t.Fatalf("expected connection to return to pool once, got %d", cmd.putConnCalls)
	}
	if netConn.closed {
		t.Fatal("clean connection was closed")
	}
}

func TestExecuteAtReturnsCleanConnectionOnWriteBufferCancellation(t *testing.T) {
	conn, netConn := newStubConnection()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.writeBufferHook = func(cmd *commandExecutionStub) Error {
		cancel()
		return ErrTimeout.err()
	}

	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if cmd.putConnCalls != 1 {
		t.Fatalf("expected connection to return to pool once, got %d", cmd.putConnCalls)
	}
	if netConn.closed {
		t.Fatal("clean connection was closed")
	}
	if netConn.writeCalls != 0 {
		t.Fatalf("expected no bytes written before cancellation, got %d writes", netConn.writeCalls)
	}
}

func TestExecuteAtCancellationBeforeWatchInstallationReturnsContextError(t *testing.T) {
	conn, netConn := newStubConnection()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.writeBufferHook = func(cmd *commandExecutionStub) Error {
		cancel()
		cmd.dataOffset = 30
		return nil
	}

	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if cmd.putConnCalls != 0 {
		t.Fatalf("canceled connection returned to pool %d times", cmd.putConnCalls)
	}
	if !netConn.closed {
		t.Fatal("canceled connection was not closed")
	}
}

func TestExecuteAtCancellationDuringBlockedReadDiscardsConnection(t *testing.T) {
	netConn := newBlockingNetConn()
	buffer := make([]byte, 64)
	conn := &Connection{
		conn:                 netConn,
		dataBuffer:           buffer,
		origDataBuffer:       buffer,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.parseResultHook = func(cmd *commandExecutionStub) Error {
		_, err := cmd.conn.Read(make([]byte, 1), 1)
		return err
	}

	done := make(chan Error, 1)
	go func() {
		done <- cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	}()

	select {
	case <-netConn.readStarted:
	case <-time.After(time.Second):
		t.Fatal("command did not block while reading the response")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked command did not return after cancellation")
	}

	if cmd.putConnCalls != 0 {
		t.Fatalf("interrupted connection returned to pool %d times", cmd.putConnCalls)
	}
	if !netConn.closed.Load() {
		t.Fatal("interrupted connection was not closed")
	}

	followUpConn, followUpNetConn := newStubConnection()
	followUp := newCommandExecutionStub(context.Background(), followUpConn)
	if err := followUp.executeAt(followUp, followUp.policy, time.Time{}, -1); err != nil {
		t.Fatalf("fresh follow-up command failed: %v", err)
	}
	if followUp.putConnCalls != 1 || followUpNetConn.closed {
		t.Fatal("fresh follow-up connection was not returned healthy")
	}
}

func TestExecuteAtCancellationDuringFinalReadDiscardsConnection(t *testing.T) {
	netConn := newFinalReadNetConn()
	buffer := make([]byte, 64)
	conn := &Connection{
		conn:                 netConn,
		dataBuffer:           buffer,
		origDataBuffer:       buffer,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.parseResultHook = func(cmd *commandExecutionStub) Error {
		_, err := cmd.conn.Read(make([]byte, 1), 1)
		return err
	}

	done := make(chan Error, 1)
	go func() {
		done <- cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	}()

	select {
	case <-netConn.readStarted:
	case <-time.After(time.Second):
		t.Fatal("command did not block while reading the final response byte")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("final-read cancellation did not return")
	}

	if cmd.putConnCalls != 0 {
		t.Fatalf("interrupted connection returned to pool %d times", cmd.putConnCalls)
	}
	if !netConn.closed.Load() {
		t.Fatal("interrupted connection was not closed")
	}

	followUpConn, followUpNetConn := newStubConnection()
	followUp := newCommandExecutionStub(context.Background(), followUpConn)
	if err := followUp.executeAt(followUp, followUp.policy, time.Time{}, -1); err != nil {
		t.Fatalf("fresh follow-up command failed: %v", err)
	}
	if followUp.putConnCalls != 1 || followUpNetConn.closed {
		t.Fatal("fresh follow-up connection was not returned healthy")
	}
}

func TestExecuteAtDeadlineDuringBlockedReadDiscardsConnection(t *testing.T) {
	netConn := newBlockingNetConn()
	buffer := make([]byte, 64)
	conn := &Connection{
		conn:                 netConn,
		dataBuffer:           buffer,
		origDataBuffer:       buffer,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.parseResultHook = func(cmd *commandExecutionStub) Error {
		_, err := cmd.conn.Read(make([]byte, 1), 1)
		return err
	}

	started := time.Now()
	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("deadline cancellation took too long: %s", elapsed)
	}
	if cmd.putConnCalls != 0 {
		t.Fatalf("interrupted connection returned to pool %d times", cmd.putConnCalls)
	}
	if !netConn.closed.Load() {
		t.Fatal("interrupted connection was not closed")
	}
}

func TestExecuteAtPreservesContextAtMaxRetriesBoundary(t *testing.T) {
	conn, _ := newStubConnection()
	ctx := &deadlineRaceContext{
		deadline: time.Now().Add(20 * time.Millisecond),
		done:     make(chan struct{}),
	}
	cmd := newCommandExecutionStub(ctx, conn)
	cmd.getConnHook = func(cmd *commandExecutionStub) (*Connection, Error) {
		time.Sleep(30 * time.Millisecond)
		return nil, ErrTimeout.err()
	}

	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestExecuteAtCancellationStopsConnectionPoolAcquisitionRetries(t *testing.T) {
	conn, _ := newStubConnection()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := newCommandExecutionStub(ctx, conn)
	acquisitionAttempts := 0
	cmd.getConnHook = func(cmd *commandExecutionStub) (*Connection, Error) {
		acquisitionAttempts++
		if acquisitionAttempts == 10 {
			cancel()
		}
		return nil, ErrConnectionPoolEmpty.err()
	}

	err := cmd.executeAt(cmd, cmd.policy, time.Time{}, -1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if acquisitionAttempts != 10 {
		t.Fatalf("expected cancellation after 10 attempts, got %d", acquisitionAttempts)
	}
	if cmd.putConnCalls != 0 {
		t.Fatalf("unexpected connection return count: %d", cmd.putConnCalls)
	}
}

func TestExecuteIterUsesEarlierContextDeadline(t *testing.T) {
	conn, _ := newStubConnection()
	contextDeadline := time.Now().Add(10 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), contextDeadline)
	defer cancel()

	cmd := newCommandExecutionStub(ctx, conn)
	cmd.policy.TotalTimeout = time.Hour

	if err := cmd.executeIter(cmd, 0); err != nil {
		t.Fatalf("expected successful command, got %v", err)
	}
	if !conn.deadline.Equal(contextDeadline) {
		t.Fatalf("expected context deadline %v, got %v", contextDeadline, conn.deadline)
	}
}

func TestSizeBufferSzTracksBorrowedBuffersAcrossResizes(t *testing.T) {
	conn, _ := newStubConnection()
	cmd := baseCommand{
		conn: conn,
		bufferEx: bufferEx{
			dataBuffer: conn.dataBuffer,
		},
	}

	if err := cmd.sizeBufferSz(len(conn.dataBuffer)+1, false); err != nil {
		t.Fatal(err)
	}
	firstBorrowed := cmd.dataBuffer
	if len(cmd.borrowedBuffers) != 1 {
		t.Fatalf("expected one borrowed buffer, got %d", len(cmd.borrowedBuffers))
	}

	if err := cmd.sizeBufferSz(cap(firstBorrowed)+1, false); err != nil {
		t.Fatal(err)
	}
	if len(cmd.borrowedBuffers) != 2 {
		t.Fatalf("expected two borrowed buffers, got %d", len(cmd.borrowedBuffers))
	}
	if len(cmd.borrowedBuffers[0]) == 0 || len(cmd.borrowedBuffers[1]) == 0 {
		t.Fatal("expected both borrowed buffers to remain tracked")
	}

	cmd.cleanupConnectionState()
	if cmd.borrowedBuffers != nil {
		t.Fatal("cleanup did not release borrowed buffers")
	}
}
