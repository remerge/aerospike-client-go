package aerospike

import (
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"
)

// deadlineProbeCommand simulates a pool miss followed by an available socket.
// It records the timeout used for that socket without requiring a server.
type deadlineProbeCommand struct {
	fakeCommand
	node     *Node
	timeouts []time.Duration
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
	_ = conn.setTimeout(policy.GetBasePolicy().TotalTimeout, policy.GetBasePolicy().SocketTimeout)
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
