package raft

import "testing"

func TestRaftLogAppend(t *testing.T) {
	log := &RaftLog{}

	log.Append(1, Command{})

	log.Append(1, Command{})

	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", log.LastIndex())
	}
}


func TestRaftLogGet(t *testing.T) {
	log := &RaftLog{}

	log.Append(LogEntry{
		Index: 1,
		Term: 1,
		Command: Command{
			Type:  CommandSet,
			Key:   "A",
			Value: "10",
		},
	})

	entry, ok := log.Get(1)

	if !ok {
		t.Fatal("expected entry")
	}

	if entry.Command.Key != "A" {
		t.Fatalf("expected key A, got %s", entry.Command.Key)
	}
}

func TestRaftLogEntriesFrom(t *testing.T) {
	log := &RaftLog{}

	for i := uint64(1); i <= 5; i++ {
		log.Append(LogEntry{
			Index: i,
			Term:  1,
		})
	}

	entries := log.EntriesFrom(3)

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Index != 3 {
		t.Fatalf("expected first index 3, got %d", entries[0].Index)
	}
}

