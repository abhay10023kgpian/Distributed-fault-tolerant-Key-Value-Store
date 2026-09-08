package raft

import (
	"sync"
	"time"
)

type State byte

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	mu sync.Mutex

	id int

	state State

	currentTerm uint64
	votedFor    int

	electionTimer *time.Timer

	log *RaftLog

	commitIndex uint64
	lastApplied uint64

	peers []*RaftNode
}


func NewRaftNode(id int) *RaftNode {
	r := &RaftNode{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         &RaftLog{},
		commitIndex: 0,
		lastApplied: 0,
	}

	r.electionTimer = time.NewTimer(randomElectionTimeout())

	return r
}

func (r *RaftNode) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader

	go r.runHeartbeatLoop()
}


func (r *RaftNode) startElection() {
	r.mu.Lock()

	r.becomeCandidate()

	args := RequestVoteArgs{
		Term:         r.currentTerm,
		LastLogIndex: r.log.LastIndex(),
	}

	if args.LastLogIndex > 0 {
		entry, _ := r.log.Get(args.LastLogIndex)
		args.LastLogTerm = entry.Term
	}

	peers := r.peers

	r.mu.Unlock()

	votes := 1 // vote for ourselves

	for _, peer := range peers {
		if peer.id == r.id {
			continue
		}

		reply := peer.RequestVote(args)

		r.mu.Lock()

		// Another node has a newer term.
		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.state = Follower
			r.votedFor = -1
			r.mu.Unlock()
			return
		}

		// We may have already lost the election to another
		// candidate/leader while this RPC was in flight.
		if r.state != Candidate || r.currentTerm != args.Term {
			r.mu.Unlock()
			return
		}

		if reply.VoteGranted {
			votes++
		}

		r.mu.Unlock()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Candidate &&
		r.currentTerm == args.Term &&
		votes >= len(r.peers)/2+1 {
		r.becomeLeader()
	}
}


func (r *RaftNode) runHeartbeatLoop() {
    ticker := time.NewTicker(50 * time.Millisecond)
    defer ticker.Stop()

    for {
        <-ticker.C

        r.mu.Lock()

        if r.state != Leader {
            r.mu.Unlock()
            return
        }

        r.mu.Unlock()

        r.sendHeartbeats()
    }
}