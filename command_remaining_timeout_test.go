package aerospike

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"slices"
	"testing"
	"time"
)

// deadlineProbeCommand simulates a pool miss followed by an available socket.
// It records the timeout used for that socket without requiring a server.
type deadlineProbeCommand struct {
	fakeCommand
	node     *Node
	timeouts []time.Duration
	policy   *BasePolicy
}

type deadlineSocketCommand struct {
	deadlineProbeCommand
	peer net.Conn
}

func (c *deadlineSocketCommand) getConnection(policy Policy) (*Connection, Error) {
	if len(c.timeouts) == 0 {
		c.timeouts = append(c.timeouts, policy.GetBasePolicy().TotalTimeout)
		time.Sleep(120 * time.Millisecond)
		return nil, ErrConnectionPoolEmpty.err()
	}
	client, peer := net.Pipe()
	c.peer = peer
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	buffer := make([]byte, 64)
	conn := &Connection{conn: client, dataBuffer: buffer, origDataBuffer: buffer}
	bp := policy.GetBasePolicy()
	if bp.Deadline.IsZero() {
		_ = conn.setTimeout(bp.TotalTimeout, bp.SocketTimeout)
	} else {
		_ = conn.SetTimeout(bp.Deadline, bp.SocketTimeout)
		conn.strictDeadline = true
	}
	return conn, nil
}

func (c *deadlineSocketCommand) writeBuffer(command) Error {
	c.dataOffset = 30
	return nil
}

func (c *deadlineSocketCommand) parseResult(_ command, conn *Connection) Error {
	_, err := conn.Read(make([]byte, 1), 1)
	return err
}

func TestCommandPoolWaitAndSocketReadShareDeadline(t *testing.T) {
	t.Run("detailed metrics disabled", func(t *testing.T) { testCommandPoolWaitAndSocketRead(t, false) })
	t.Run("detailed metrics enabled", func(t *testing.T) { testCommandPoolWaitAndSocketRead(t, true) })
}

func testCommandPoolWaitAndSocketRead(t *testing.T, detailedMetrics bool) {
	t.Helper()
	cluster := &Cluster{}
	cluster.metricsEnabled.Store(detailedMetrics)
	node := &Node{cluster: cluster, host: NewHost("127.0.0.1", 3000), stats: *newNodeStats(DefaultMetricsPolicy())}
	node.active.Set(true)
	cmd := &deadlineSocketCommand{deadlineProbeCommand: deadlineProbeCommand{node: node}}
	policy := NewPolicy()
	policy.TotalTimeout = 200 * time.Millisecond
	policy.MaxRetries = 0
	policy.SleepBetweenRetries = 0
	start := time.Now()
	err := cmd.baseCommand.executeAt(cmd, policy, start.Add(policy.TotalTimeout), -1)
	elapsed := time.Since(start)
	if cmd.peer != nil {
		_ = cmd.peer.Close()
	}
	if err == nil {
		t.Fatal("expected a timeout waiting for the response")
	}
	if elapsed > 280*time.Millisecond {
		t.Fatalf("pool wait and socket read took %s, exceeding the 200ms budget", elapsed)
	}
	if got := node.stats.DeadlinePoolWaitSocketAttempts.Get(); got != 1 {
		t.Fatalf("pool-wait socket attempts = %d, want 1", got)
	}
	if got := node.stats.DeadlinePoolWaitSocketTimeouts.Get(); got != 1 {
		t.Fatalf("pool-wait socket timeouts = %d, want 1", got)
	}
	aggregated := newNodeStats(DefaultMetricsPolicy())
	aggregated.aggregate(node.stats.getAndReset())
	cloned := aggregated.clone()
	encoded, marshalErr := json.Marshal(cloned)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var values map[string]any
	if unmarshalErr := json.Unmarshal(encoded, &values); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	for _, key := range []string{"deadline-pool-wait-socket-attempts", "deadline-pool-wait-socket-timeouts"} {
		if values[key] != float64(1) {
			t.Fatalf("exported %s = %v, want 1", key, values[key])
		}
	}
}

func (c *deadlineProbeCommand) getNode(command) (*Node, Error) { return c.node, nil }

func (c *deadlineProbeCommand) getPolicy(command) Policy { return c.policy }

func TestCommandsShareAbsoluteDeadline(t *testing.T) {
	node := &Node{cluster: &Cluster{}, host: NewHost("127.0.0.1", 3000)}
	node.active.Set(true)
	policy := NewPolicy()
	policy.Deadline = time.Now().Add(time.Second)
	policy.TotalTimeout = 0
	policy.MaxRetries = 0
	policy.ExitFastOnExhaustedConnectionPool = true
	var events []DeadlineEvent
	policy.ObserveDeadline = func(event DeadlineEvent) { events = append(events, event) }

	first := &deadlineProbeCommand{node: node, policy: policy}
	if err := first.baseCommand.execute(first); !errors.Is(err, ErrConnectionPoolExhausted) {
		t.Fatalf("expected pool exhaustion, got %v", err)
	}
	second := &deadlineProbeCommand{node: node, policy: policy}
	_ = second.baseCommand.execute(second)
	if len(first.timeouts) != 1 || len(second.timeouts) != 1 {
		t.Fatalf("expected one connection attempt per command, got %v and %v", first.timeouts, second.timeouts)
	}
	if saved := first.timeouts[0] - second.timeouts[0]; saved < 80*time.Millisecond {
		t.Fatalf("second command did not consume the same budget: first=%s second=%s", first.timeouts[0], second.timeouts[0])
	}
	if !slices.Equal(events, []DeadlineEvent{
		DeadlineCommandStarted, DeadlineSelected, DeadlinePoolFailedFast,
		DeadlineCommandStarted, DeadlineSelected, DeadlinePoolFailedFast,
	}) {
		t.Fatalf("unexpected execution events: %v", events)
	}

	events = nil
	expiredPolicy := *policy
	expiredPolicy.Deadline = time.Now().Add(-time.Second)
	expired := &deadlineProbeCommand{node: node, policy: &expiredPolicy}
	if err := expired.baseCommand.execute(expired); err == nil || len(expired.timeouts) != 0 {
		t.Fatalf("expired command attempted a connection: attempts=%v error=%v", expired.timeouts, err)
	}
	if !slices.Equal(events, []DeadlineEvent{DeadlineCommandStarted, DeadlineSelected, DeadlineExpiredBeforeIO}) {
		t.Fatalf("unexpected expired command events: %v", events)
	}
}

func TestRequestDeadlineReportsSocketTimeout(t *testing.T) {
	t.Run("shared request deadline", func(t *testing.T) {
		testRequestDeadlineSocketEvents(t, time.Second, DeadlineSocketTimedOut)
	})
	t.Run("shorter socket timeout", func(t *testing.T) {
		testRequestDeadlineSocketEvents(t, 10*time.Millisecond, DeadlineRetrySuppressed)
	})
}

func testRequestDeadlineSocketEvents(t *testing.T, socketTimeout time.Duration, want DeadlineEvent) {
	t.Helper()
	node := &Node{
		cluster: &Cluster{},
		host:    NewHost("127.0.0.1", 3000),
		stats:   *newNodeStats(DefaultMetricsPolicy()),
	}
	node.active.Set(true)
	policy := NewPolicy()
	policy.Deadline = time.Now().Add(250 * time.Millisecond)
	policy.SocketTimeout = socketTimeout
	policy.MaxRetries = 0
	policy.SleepBetweenRetries = 0
	var events []DeadlineEvent
	policy.ObserveDeadline = func(event DeadlineEvent) { events = append(events, event) }
	cmd := &deadlineSocketCommand{deadlineProbeCommand: deadlineProbeCommand{node: node, policy: policy}}
	err := cmd.baseCommand.execute(cmd)
	if cmd.peer != nil {
		_ = cmd.peer.Close()
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected socket timeout, got %v", err)
	}
	if !slices.Equal(events, []DeadlineEvent{DeadlineCommandStarted, DeadlineSelected, want}) {
		t.Fatalf("unexpected stalled socket events: %v", events)
	}
}

type failedConnectionCommand struct{ deadlineProbeCommand }

func (c *failedConnectionCommand) getConnection(policy Policy) (*Connection, Error) {
	c.timeouts = append(c.timeouts, policy.GetBasePolicy().TotalTimeout)
	return nil, ErrServerNotAvailable.err()
}

func TestDeadlinePolicyReportsSuppressedRetry(t *testing.T) {
	node := &Node{cluster: &Cluster{}, host: NewHost("127.0.0.1", 3000)}
	node.active.Set(true)
	policy := NewPolicy()
	policy.Deadline = time.Now().Add(time.Minute)
	policy.MaxRetries = 0
	var events []DeadlineEvent
	policy.ObserveDeadline = func(event DeadlineEvent) { events = append(events, event) }
	cmd := &failedConnectionCommand{deadlineProbeCommand: deadlineProbeCommand{node: node, policy: policy}}
	_ = cmd.baseCommand.execute(cmd)
	if len(cmd.timeouts) != 1 {
		t.Fatalf("expected one attempt, got %v", cmd.timeouts)
	}
	// The shorter default TotalTimeout wins, so no DeadlineSelected event.
	if !slices.Equal(events, []DeadlineEvent{DeadlineCommandStarted, DeadlineRetrySuppressed}) {
		t.Fatalf("unexpected events after a failed connection: %v", events)
	}
}

func (c *deadlineProbeCommand) getConnection(policy Policy) (*Connection, Error) {
	c.timeouts = append(c.timeouts, policy.GetBasePolicy().TotalTimeout)
	if len(c.timeouts) == 1 {
		time.Sleep(80 * time.Millisecond)
		return nil, ErrConnectionPoolEmpty.err()
	}
	return nil, ErrServerNotAvailable.err()
}

func TestCommandConnectionTimeoutUsesRemainingBudget(t *testing.T) {
	cluster := &Cluster{}
	node := &Node{cluster: cluster, host: NewHost("127.0.0.1", 3000), stats: *newNodeStats(DefaultMetricsPolicy())}
	node.active.Set(true)
	cmd := &deadlineProbeCommand{node: node}
	policy := NewPolicy()
	policy.TotalTimeout = 200 * time.Millisecond
	policy.ExitFastOnExhaustedConnectionPool = false
	policy.MaxRetries = 0
	policy.SleepBetweenRetries = 0
	err := cmd.baseCommand.executeAt(cmd, policy, time.Now().Add(policy.TotalTimeout), -1)
	if len(cmd.timeouts) != 2 {
		t.Fatalf("got %d connection attempts, want 2: %v", len(cmd.timeouts), err)
	}
	if cmd.timeouts[1] > 150*time.Millisecond {
		t.Fatalf("socket timeout after pool wait = %s, want remaining budget below 150ms", cmd.timeouts[1])
	}
}
