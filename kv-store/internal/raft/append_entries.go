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

	// Leader is from an older term.
	if args.Term < r.currentTerm {
		return reply
	}

	// Leader is from a newer term.
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
	}

	// A valid leader exists for this term.
	r.state = Follower
	r.resetElectionTimer()

	reply.Term = r.currentTerm

	// Check that the previous log entry exists.
	if args.PrevLogIndex > 0 {
		if uint64(args.PrevLogIndex) > r.log.LastIndex() {
			return reply
		}

		entry, ok := r.log.Get(uint64(args.PrevLogIndex))
		if !ok {
			return reply
		}

		// Previous entry must have the same term.
		if entry.Term != uint64(args.PrevLogTerm) {
			return reply
		}
	}

	// Previous log entry matches.
	reply.Success = true

		// Append new entries to the log.
	for _, entry := range args.Entries {
		r.log.Entries = append(r.log.Entries, entry)
	}
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

	peers := r.peers

	for i, peer := range peers {
		if peer.id == r.id {
			continue
		}

		next := r.nextIndex[i]

		prevLogIndex := uint64(0)
		var prevLogTerm uint64

		if next > 1 {
			prevLogIndex = next - 1

			entry, ok := r.log.Get(prevLogIndex)
			if ok {
				prevLogTerm = entry.Term
			}
		}

		entries := r.log.EntriesFrom(next)

		args := AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.id,
			PrevLogIndex: int(prevLogIndex),
			PrevLogTerm:  int(prevLogTerm),
			Entries:      entries,
			LeaderCommit: int(r.commitIndex),
		}

		r.mu.Unlock()

		reply := peer.AppendEntries(args)

		r.mu.Lock()

		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.state = Follower
			r.votedFor = -1
			r.mu.Unlock()
			return
		}
	}

	r.mu.Unlock()
}