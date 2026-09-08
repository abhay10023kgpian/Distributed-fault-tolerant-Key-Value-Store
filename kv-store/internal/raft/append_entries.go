package raft

import (
	"math/rand"
	"time"
)

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

type AppendEntriesArgs struct {
	Term     uint64
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

func (r *RaftNode) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply := AppendEntriesReply{
		Term: r.currentTerm,
	}

	if args.Term < r.currentTerm {
		return reply
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
	}

	r.state = Follower

	r.resetElectionTimer()

	reply.Term = r.currentTerm
	reply.Success = true

	return reply
}

func (r *RaftNode) resetElectionTimer() {
	r.electionTimer.Stop()

	r.electionTimer.Reset(randomElectionTimeout())
}

func (r *RaftNode) runElectionTimer() {
	for {
		<-r.electionTimer.C

		r.mu.Lock()

		if r.state == Leader {
			r.mu.Unlock()
			continue
		}

		r.mu.Unlock()

		r.startElection()
	}
}


func (r *RaftNode) sendHeartbeats() {
	r.mu.Lock()

	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		Entries:      nil,
		LeaderCommit: int(r.commitIndex),
	}

	peers := r.peers

	r.mu.Unlock()

	for _, peer := range peers {
		if peer.id == r.id {
			continue
		}

		reply := peer.AppendEntries(args)

		r.mu.Lock()

		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.state = Follower
			r.votedFor = -1
			r.mu.Unlock()
			return
		}

		r.mu.Unlock()
	}
}