package raft

import (
	"testing"
	"time"
)

func TestThreeNodeReplication(t *testing.T) {
	nodes := []*RaftNode{
		NewRaftNode(0),
		NewRaftNode(1),
		NewRaftNode(2),
	}

	// Connect all nodes to each other.
	for _, node := range nodes {
		node.peers = nodes
	}

	// Record commands that each node applies.
	applied := make([]chan Command, len(nodes))

	for i, node := range nodes {
		applied[i] = make(chan Command, 10)

		ch := applied[i]

		node.SetApplyFunc(func(cmd Command) {
			ch <- cmd
		})

		go node.runElectionTimer()
	}

	// Wait for leader election.
	var leader *RaftNode

	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)

		for _, node := range nodes {
			node.mu.Lock()

			if node.state == Leader {
				leader = node
			}

			node.mu.Unlock()
		}

		if leader != nil {
			break
		}
	}

	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Submit a command to the leader.
	command := Command{
		Type:  CommandSet,
		Key:   "foo",
		Value: "bar",
	}

	index, _, ok := leader.Start(command)

	if !ok {
		t.Fatal("leader rejected command")
	}

	if index != 1 {
		t.Fatalf("expected command at index 1, got %d", index)
	}

	// Give heartbeats time to replicate and commit.
	time.Sleep(300 * time.Millisecond)

	// Verify all nodes received the command in their log.
	for _, node := range nodes {
		node.mu.Lock()

		entry, ok := node.log.Get(1)

		if !ok {
			node.mu.Unlock()
			t.Fatalf("node %d does not have log entry", node.id)
		}

		if entry.Command != command {
			node.mu.Unlock()
			t.Fatalf("node %d has wrong command: %+v", node.id, entry.Command)
		}

		if node.commitIndex != 1 {
			node.mu.Unlock()
			t.Fatalf(
				"node %d expected commitIndex=1, got %d",
				node.id,
				node.commitIndex,
			)
		}

		node.mu.Unlock()
	}

	// Verify every node applied the command.
	for i := range nodes {
		select {
		case appliedCommand := <-applied[i]:
			if appliedCommand != command {
				t.Fatalf(
					"node %d applied wrong command: %+v",
					i,
					appliedCommand,
				)
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("node %d did not apply command", i)
		}
	}
}