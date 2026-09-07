package raft

type State byte

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	id int

	state State

	currentTerm uint64
	votedFor    int

	log *RaftLog

	commitIndex uint64
	lastApplied uint64
}

