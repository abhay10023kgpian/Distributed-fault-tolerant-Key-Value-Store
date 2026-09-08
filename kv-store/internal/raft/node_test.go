package raft

import "testing"

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode(1)

	if node.id != 1 {
		t.Fatalf("expected id 1, got %d", node.id)
	}

	if node.state != Follower {
		t.Fatalf("expected follower state")
	}

	if node.currentTerm != 0 {
		t.Fatalf("expected term 0, got %d", node.currentTerm)
	}

	if node.votedFor != -1 {
		t.Fatalf("expected no vote, got %d", node.votedFor)
	}

	if node.commitIndex != 0 {
		t.Fatalf("expected commitIndex 0")
	}

	if node.lastApplied != 0 {
		t.Fatalf("expected lastApplied 0")
	}
}

func TestBecomeCandidate(t *testing.T) {
	node := NewRaftNode(1)

	node.becomeCandidate()

	if node.state != Candidate {
		t.Fatalf("expected candidate state")
	}

	if node.currentTerm != 1 {
		t.Fatalf("expected term 1, got %d", node.currentTerm)
	}

	if node.votedFor != 1 {
		t.Fatalf("expected self vote, got %d", node.votedFor)
	}
}

func TestBecomeLeader(t *testing.T) {
	node := NewRaftNode(1)

	node.becomeCandidate()
	node.becomeLeader()

	if node.state != Leader {
		t.Fatalf("expected leader state")
	}
}


func TestLeaderSendsHeartbeat(t *testing.T) {
	n1 := NewRaftNode(1)
	n2 := NewRaftNode(2)

	n1.peers = []*RaftNode{n1, n2}
	n2.peers = []*RaftNode{n1, n2}

	n1.currentTerm = 1
	n1.becomeLeader()

	n2.currentTerm = 1
	n2.state = Follower

	n1.sendHeartbeats()

	n2.mu.Lock()
	defer n2.mu.Unlock()

	if n2.state != Follower {
		t.Fatalf("expected follower, got %v", n2.state)
	}
}


func TestApplyCommitted(t *testing.T) {
	node := NewRaftNode(1)

	applied := make([]Command, 0)

	node.SetApplyFunc(func(cmd Command) {
		applied = append(applied, cmd)
	})

	node.currentTerm = 1

	node.log.Append(1, Command{
		Type:  CommandSet,
		Key:   "name",
		Value: "Abhay",
	})

	node.log.Append(1, Command{
		Type:  CommandSet,
		Key:   "age",
		Value: "21",
	})

	node.commitIndex = 2

	node.applyCommitted()

	if node.lastApplied != 2 {
		t.Fatalf("expected lastApplied=2, got %d", node.lastApplied)
	}

	if len(applied) != 2 {
		t.Fatalf("expected 2 applied commands, got %d", len(applied))
	}

	if applied[0].Key != "name" {
		t.Fatalf("expected first command key=name, got %s", applied[0].Key)
	}

	if applied[1].Key != "age" {
		t.Fatalf("expected second command key=age, got %s", applied[1].Key)
	}
}