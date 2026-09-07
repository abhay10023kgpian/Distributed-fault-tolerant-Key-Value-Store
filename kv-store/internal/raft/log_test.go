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

	log.Append(1, Command{
		Type:  CommandSet,
		Key:   "A",
		Value: "10",
	},
	)

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
		log.Append(i, Command{})
	}

	entries := log.EntriesFrom(3)

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Index != 3 {
		t.Fatalf("expected first index 3, got %d", entries[0].Index)
	}
}



func TestRaftLogTruncate(t *testing.T) {
	log := &RaftLog{}

	log.Append(1, Command{
		Type:  CommandSet,
		Key:   "A",
		Value: "10",
	})

	log.Append(1, Command{
		Type:  CommandSet,
		Key:   "B",
		Value: "20",
	})

	log.Append(2, Command{
		Type:  CommandSet,
		Key:   "C",
		Value: "30",
	})

	log.Append(2, Command{
		Type:  CommandSet,
		Key:   "D",
		Value: "40",
	})

	log.TruncateFrom(3)

	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", log.LastIndex())
	}
}

func TestRaftLogConflict(t *testing.T) {
	log := &RaftLog{}

	log.Append(1, Command{Key: "A"})
	log.Append(1, Command{Key: "B"})
	log.Append(2, Command{Key: "X"})
	log.Append(2, Command{Key: "Y"})
	log.Append(2, Command{Key: "Z"})

	// Leader's entry at index 3 has a different term.
	entry, ok := log.Get(3)
	if !ok {
		t.Fatal("expected entry at index 3")
	}

	if entry.Term == 3 {
		t.Fatal("test setup is invalid")
	}

	// Remove conflicting suffix.
	log.TruncateFrom(3)

	// Replicate leader's entries.
	log.Append(3, Command{Key: "C"})
	log.Append(3, Command{Key: "D"})

	if log.LastIndex() != 4 {
		t.Fatalf("expected last index 4, got %d", log.LastIndex())
	}

	entry, ok = log.Get(3)
	if !ok {
		t.Fatal("expected entry at index 3")
	}

	if entry.Term != 3 {
		t.Fatalf("expected term 3, got %d", entry.Term)
	}
}