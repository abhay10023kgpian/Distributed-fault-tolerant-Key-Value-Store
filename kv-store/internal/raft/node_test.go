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