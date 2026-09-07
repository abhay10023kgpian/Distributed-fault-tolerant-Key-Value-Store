package raft

type CommandType byte

const (
	CommandSet CommandType = iota
	CommandDelete
)

type Command struct {
	Type  CommandType
	Key   string
	Value string
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

type RaftLog struct {
	entries []LogEntry
}

func (l *RaftLog) Append(term uint64, command Command) LogEntry {
	entry := LogEntry{
		Index: l.LastIndex() + 1,
		Term:  term,
		Command: command,
	}

	l.entries = append(l.entries, entry)

	return entry
}

func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) Get(index uint64) (LogEntry, bool) {
	if index == 0 || index > uint64(len(l.entries)) {
		return LogEntry{}, false
	}

	return l.entries[index-1], true
}

func (l *RaftLog) EntriesFrom(index uint64) []LogEntry {
	if index == 0 || index > uint64(len(l.entries)) {
		return nil
	}

	return l.entries[index-1:]
}

func (l *RaftLog) TruncateFrom(index uint64) {
	if index == 0 || index > uint64(len(l.entries)) {
		return
	}

	l.entries = l.entries[:index-1]
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