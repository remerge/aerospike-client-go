package aerospike

import (
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
