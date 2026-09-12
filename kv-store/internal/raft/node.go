package raft

import (
	"fmt"
	"kv-store/internal/events"
	"sync"
	"time"	
)

type State byte

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type RaftNode struct {
	mu sync.Mutex

	id          int
	state       State
	currentTerm uint64
	votedFor    int

	electionTimer *time.Timer

	log *RaftLog

	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	commitIndex uint64
	lastApplied uint64

	peers map[int]string

	transport Transport

	applyFunc func(Command)

	stopCh chan struct{}

	leaderID int

	eventBus *events.Bus
}


func NewRaftNode(id int) *RaftNode {
	r := &RaftNode{
		id:           id,
		state:        Follower,
		currentTerm:  0,
		votedFor:     -1,
		log:          &RaftLog{},
		commitIndex:  0,
		lastApplied:  0,
		stopCh:       make(chan struct{}),
		leaderID: -1,
	}

	r.electionTimer = time.NewTimer(randomElectionTimeout())

	return r
}

func (r *RaftNode) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.emitEvent(events.BecameCandidate, "Node became candidate", -1, 0)
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader
	r.leaderID = r.id

	lastIndex := r.log.LastIndex()

	r.nextIndex = make(map[int]uint64)
	r.matchIndex = make(map[int]uint64)

	for id := range r.peers {
		r.nextIndex[id] = lastIndex + 1
		r.matchIndex[id] = 0
	}

	r.emitEvent(events.BecameLeader, "Node became leader", -1, 0)

	go r.runHeartbeatLoop()
}

func (r *RaftNode) LeaderID() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderID
}


func (r *RaftNode) startElection() {
	r.mu.Lock()

	r.becomeCandidate()
    r.resetElectionTimer()

	r.emitEvent(events.ElectionStarted, "Election started", -1, 0)

	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: r.log.LastIndex(),
	}

	if args.LastLogIndex > 0 {
		entry, _ := r.log.Get(args.LastLogIndex)
		args.LastLogTerm = entry.Term
	}

	peers := r.peers

	r.mu.Unlock()

	votes := 1 // vote for ourselves

	for id, address := range peers {
		if id == r.id {
			continue
		}

		go func(peerID int, peerAddress string) {
			reply, err := r.transport.RequestVote(peerAddress, args)
			if err != nil {
				// Network error, peer is unreachable
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			// We may have already lost the election to another
			// candidate/leader while this RPC was in flight.
			if r.state != Candidate || r.currentTerm != args.Term {
				return
			}

			// Another node has a newer term.
			if reply.Term > r.currentTerm {
				r.currentTerm = reply.Term
				r.state = Follower
				r.votedFor = -1
				r.emitEvent(events.BecameFollower, "Stepped down: discovered higher term", peerID, 0)
				return
			}

			if reply.VoteGranted {
				votes++
				r.emitEvent(events.VoteGranted, "Vote granted", peerID, 0)

				if r.state == Candidate &&
					r.currentTerm == args.Term &&
					votes >= len(r.peers)/2+1 {
					r.becomeLeader()
				}
			}
		}(id, address)
	}
}


func (r *RaftNode) runHeartbeatLoop() {
    ticker := time.NewTicker(150 * time.Millisecond)
    defer ticker.Stop()

    for {
		
		select {
		case <-r.stopCh:
			return

		case <-ticker.C:
			r.sendHeartbeats()
		}

        r.mu.Lock()

        if r.state != Leader {
            r.mu.Unlock()
            return
        }

        r.mu.Unlock()

        r.sendHeartbeats()
    }
}


func (r *RaftNode) updateCommitIndex() {
	for index := r.commitIndex + 1; index <= r.log.LastIndex(); index++ {
		count := 1 // leader itself

		for id := range r.peers {
			if id == r.id {
				continue
			}

			if r.matchIndex[id] >= index {
				count++
			}
		}

		if count > len(r.peers)/2 {
			r.commitIndex = index
			r.emitEvent(events.CommitAdvanced, "Commit index advanced", -1, r.commitIndex)
		}
	}
}


func (r *RaftNode) applyCommitted() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++

		entry, ok := r.log.Get(r.lastApplied)
		if !ok {
			return
		}

		if r.applyFunc != nil {
			r.applyFunc(entry.Command)
			r.emitEvent(events.CommandApplied, "Command applied", -1, r.lastApplied)
		}
	}
}

func (r *RaftNode) SetApplyFunc(fn func(Command)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.applyFunc = fn
}



func (r *RaftNode) Start(command Command) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, r.currentTerm, false
	}

	entry := r.log.Append(r.currentTerm, command)

	r.emitEvent(events.LogAppended, fmt.Sprintf("Appended %v %s at index %d", command.Type, command.Key, entry.Index), -1, entry.Index)

	return entry.Index, r.currentTerm, true
}


func (r *RaftNode) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stopCh:
		// Already stopped.
	default:
		close(r.stopCh)
		r.emitEvent(events.NodeStopped, "Node stopped", -1, 0)
	}

	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
}

// --- Read-only getters for observability (safe, lock-protected) ---

// ID returns the node's ID.
func (r *RaftNode) ID() int {
	return r.id // immutable, no lock needed
}

// GetState returns the current Raft state.
func (r *RaftNode) GetState() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// CurrentTerm returns the current term.
func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// CommitIndex returns the commit index.
func (r *RaftNode) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// LastApplied returns the last applied index.
func (r *RaftNode) LastApplied() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastApplied
}

// LastLogIndex returns the last log index.
func (r *RaftNode) LastLogIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.log.LastIndex()
}

// IsStopped returns true if the node has been stopped.
func (r *RaftNode) IsStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isStoppedLocked()
}

// isStoppedLocked checks the stop channel without acquiring r.mu.
// Caller MUST hold r.mu.
func (r *RaftNode) isStoppedLocked() bool {
	select {
	case <-r.stopCh:
		return true
	default:
		return false
	}
}

// SetPeers sets the peer list. Used during cluster initialization.
func (r *RaftNode) SetPeers(peers map[int]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = peers
}

func (r *RaftNode) SetTransport(t Transport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transport = t
}

// RunElectionTimer is an exported wrapper for tests/server startup.
func (r *RaftNode) RunElectionTimer() {
	r.runElectionTimer()
}

// SetEventBus sets the event bus for this node.
func (r *RaftNode) SetEventBus(bus *events.Bus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.eventBus = bus
}

// emitEvent publishes an event if the event bus is set.
// Must be called with r.mu held or from a context where the fields are safe to read.
func (r *RaftNode) emitEvent(eventType events.EventType, msg string, peerID int, logIndex uint64) {
	if r.eventBus != nil {
		r.eventBus.Publish(events.Event{
			Timestamp: time.Now(),
			NodeID:    r.id,
			Type:      eventType,
			Term:      r.currentTerm,
			Message:   msg,
			PeerID:    peerID,
			LogIndex:  logIndex,
		})
	}
}

// NodeStatus is a snapshot of node state for the HTTP API.
type NodeStatus struct {
	ID           int    `json:"id"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	LeaderID     int    `json:"leader_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	Alive        bool   `json:"alive"`
}

// Status returns a read-only snapshot of the node's current state.
func (r *RaftNode) Status() NodeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return NodeStatus{
		ID:           r.id,
		State:        r.state.String(),
		Term:         r.currentTerm,
		LeaderID:     r.leaderID,
		LastLogIndex: r.log.LastIndex(),
		CommitIndex:  r.commitIndex,
		LastApplied:  r.lastApplied,
		Alive:        !r.isStoppedLocked(),
	}
}

// Restart brings a stopped node back online without losing its state
func (r *RaftNode) Restart() {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stopCh:
		// It is stopped, we can restart it.
		r.stopCh = make(chan struct{})
		r.state = Follower
		r.votedFor = -1
		
		r.emitEvent(events.NodeRestarted, "Node restarted", -1, 0)
		
		r.electionTimer.Reset(randomElectionTimeout())
		go r.runElectionTimer()
		go r.runHeartbeatLoop()
	default:
		// Not stopped.
	}
}
