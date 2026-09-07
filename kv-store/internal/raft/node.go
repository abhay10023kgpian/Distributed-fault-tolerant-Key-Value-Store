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


func NewRaftNode(id int) *RaftNode {
	return &RaftNode{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         &RaftLog{},
		commitIndex: 0,
		lastApplied: 0,
	}
}

func (r *RaftNode) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader
}