package events

import (
	"sync"
	"time"
)

// EventType represents the type of Raft event.
type EventType string

const (
	ElectionStarted      EventType = "election_started"
	VoteGranted          EventType = "vote_granted"
	VoteRejected         EventType = "vote_rejected"
	BecameLeader         EventType = "became_leader"
	BecameFollower       EventType = "became_follower"
	BecameCandidate      EventType = "became_candidate"
	HeartbeatSent        EventType = "heartbeat_sent"
	HeartbeatReceived    EventType = "heartbeat_received"
	AppendEntriesSent    EventType = "append_entries_sent"
	AppendEntriesSuccess EventType = "append_entries_success"
	AppendEntriesFailed  EventType = "append_entries_failed"
	LogAppended          EventType = "log_appended"
	LogReplicated        EventType = "log_replicated"
	CommitAdvanced       EventType = "commit_advanced"
	CommandApplied       EventType = "command_applied"
	NodeStopped          EventType = "node_stopped"
	NodeRestarted        EventType = "node_restarted"
	LeaderChanged        EventType = "leader_changed"
)

// Event is a structured event for UI observability.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	NodeID    int       `json:"node_id"`
	Type      EventType `json:"type"`
	Term      uint64    `json:"term"`
	Message   string    `json:"message"`
	PeerID    int       `json:"peer_id"`   // Use -1 for none
	LogIndex  uint64    `json:"log_index"` // Use 0 for none
}

// Subscriber receives events via a buffered channel.
type Subscriber struct {
	Ch chan Event
}

// Bus is a lightweight, thread-safe event bus.
// Events are broadcast to all subscribers without blocking the caller.
type Bus struct {
	mu          sync.RWMutex
	subscribers []*Subscriber

	// recent keeps a bounded window of recent events for new SSE clients.
	recent    []Event
	maxRecent int
}

// NewBus creates a new event bus.
func NewBus() *Bus {
	return &Bus{
		maxRecent: 1000,
	}
}

// Subscribe registers a new subscriber and returns it.
// The subscriber's channel is buffered to avoid blocking publishers.
func (b *Bus) Subscribe() *Subscriber {
	sub := &Subscriber{
		Ch: make(chan Event, 256),
	}

	b.mu.Lock()
	b.subscribers = append(b.subscribers, sub)
	b.mu.Unlock()

	return sub
}

// Unsubscribe removes a subscriber.
func (b *Bus) Unsubscribe(sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, s := range b.subscribers {
		if s == sub {
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			close(s.Ch)
			return
		}
	}
}

// Publish sends an event to all subscribers.
// If a subscriber's buffer is full, the event is dropped for that subscriber
// to avoid blocking Raft operations.
func (b *Bus) Publish(e Event) {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}

	b.mu.Lock()
	// Store in recent events ring.
	b.recent = append(b.recent, e)
	if len(b.recent) > b.maxRecent {
		b.recent = b.recent[len(b.recent)-b.maxRecent:]
	}
	// Copy subscriber list under lock.
	subs := make([]*Subscriber, len(b.subscribers))
	copy(subs, b.subscribers)
	b.mu.Unlock()

	for _, sub := range subs {
		select {
		case sub.Ch <- e:
		default:
			// Drop event for slow subscriber — never block Raft.
		}
	}
}

// Recent returns recent events for catch-up.
func (b *Bus) Recent() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Event, len(b.recent))
	copy(result, b.recent)
	return result
}
