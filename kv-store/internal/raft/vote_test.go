package raft
import "testing"

func TestRequestVoteGranted(t *testing.T) {
	node := NewRaftNode(1)

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if node.votedFor != 2 {
		t.Fatalf("expected vote for node 2, got %d", node.votedFor)
	}
}


func TestRequestVoteRejectsSecondCandidate(t *testing.T) {
	node := NewRaftNode(1)

	first := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: 2,
	})

	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: 3,
	})

	if second.VoteGranted {
		t.Fatal("expected second vote to be rejected")
	}
}


func TestRequestVoteRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode(1)

	node.currentTerm = 5

	reply := node.RequestVote(RequestVoteArgs{
		Term:        4,
		CandidateID: 2,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected")
	}

	if reply.Term != 5 {
		t.Fatalf("expected term 5, got %d", reply.Term)
	}
}


func TestRequestVoteUpdatesHigherTerm(t *testing.T) {
	node := NewRaftNode(1)

	node.currentTerm = 3
	node.votedFor = 2

	reply := node.RequestVote(RequestVoteArgs{
		Term:        4,
		CandidateID: 3,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if node.currentTerm != 4 {
		t.Fatalf("expected term 4, got %d", node.currentTerm)
	}

	if node.state != Follower {
		t.Fatal("expected follower state")
	}

	if node.votedFor != 3 {
		t.Fatalf("expected vote for node 3, got %d", node.votedFor)
	}
}

func TestRequestVoteRejectsOutdatedLog(t *testing.T) {
	node := NewRaftNode(1)

	node.log.Append(1, Command{Key: "A"})
	node.log.Append(2, Command{Key: "B"})

	reply := node.RequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 2,
		LastLogTerm:  1,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected")
	}
}

