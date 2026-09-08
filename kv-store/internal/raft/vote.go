package raft

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

func (r *RaftNode) RequestVote(args RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{
		Term: r.currentTerm,
	}

	// Candidate is from an older term.
	if args.Term < r.currentTerm {
		return reply
	}

	// Candidate is from a newer term.
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.state = Follower
		r.votedFor = -1
	}

	reply.Term = r.currentTerm

	// Already voted for somebody else in this term.
	if r.votedFor != -1 && r.votedFor != args.CandidateID {
		return reply
	}

	if !r.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return reply
	}

	r.votedFor = args.CandidateID
	r.resetElectionTimer()

	reply.VoteGranted = true

	return reply
}


func (r *RaftNode) isCandidateLogUpToDate(
	lastLogIndex uint64,
	lastLogTerm uint64,
) bool {
	lastIndex := r.log.LastIndex()

	var lastTerm uint64

	if lastIndex > 0 {
		entry, _ := r.log.Get(lastIndex)
		lastTerm = entry.Term
	}

	if lastLogTerm != lastTerm {
		return lastLogTerm > lastTerm
	}

	return lastLogIndex >= lastIndex
}
