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
	Entries []LogEntry
}

func (l *RaftLog) Append(term uint64, command Command) LogEntry {
	entry := LogEntry{
		Index:   l.LastIndex() + 1,
		Term:    term,
		Command: command,
	}

	l.Entries = append(l.Entries, entry)

	return entry
}

func (l *RaftLog) LastIndex() uint64 {
	if len(l.Entries) == 0 {
		return 0
	}

	return l.Entries[len(l.Entries)-1].Index
}

func (l *RaftLog) Get(index uint64) (LogEntry, bool) {
	if index == 0 || index > uint64(len(l.Entries)) {
		return LogEntry{}, false
	}

	return l.Entries[index-1], true
}

func (l *RaftLog) EntriesFrom(index uint64) []LogEntry {
	if index == 0 || index > uint64(len(l.Entries)) {
		return nil
	}

	return l.Entries[index-1:]
}

func (l *RaftLog) TruncateFrom(index uint64) {
	if index == 0 || index > uint64(len(l.Entries)) {
		return
	}

	l.Entries = l.Entries[:index-1]
}
