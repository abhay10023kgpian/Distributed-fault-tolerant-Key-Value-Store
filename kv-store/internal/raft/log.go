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

