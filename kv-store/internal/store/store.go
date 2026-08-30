package store

import (
	"sync"
	"kv-store/internal/wal"
)

type Store struct {
    mu   sync.RWMutex
    data map[string]string
    wal  *wal.WAL

	applyMu      sync.Mutex
	nextApplySeq uint64
	pending      map[uint64]wal.Record
}

func New(w *wal.WAL) (*Store, error) {
	data := make(map[string]string)

	records,err := w.Replay()
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		switch record.Op {
		case wal.OpPut:
			data[string(record.Key)] = string(record.Value)
		case wal.OpDelete:
			delete(data, string(record.Key))
		}
	}
	
	return &Store{
		data:         data,
		wal:          w,
		nextApplySeq: 1,
		pending:      make(map[uint64]wal.Record),
	}, nil
}

func (s *Store) Set(key, value string) error {
	record := wal.Record{
		Op:    wal.OpPut,
		Key:   []byte(key),
		Value: []byte(value),
	}

	seq, err := s.wal.Append(record)
	if err != nil {
		return err
	}

	s.applyOrdered(seq, record)

	return nil
}

func (s * Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Delete(key string) (bool, error) {
	s.mu.RLock()
	_, existed := s.data[key]
	s.mu.RUnlock()

	record := wal.Record{
		Op:  wal.OpDelete,
		Key: []byte(key),
	}

	seq, err := s.wal.Append(record)
	if err != nil {
		return false, err
	}

	s.applyOrdered(seq, record)

	return existed, nil
}


func (s *Store) applyOrdered(seq uint64, record wal.Record) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	s.pending[seq] = record

	for {
		record, ok := s.pending[s.nextApplySeq]
		if !ok {
			break
		}

		s.mu.Lock()

		switch record.Op {
		case wal.OpPut:
			s.data[string(record.Key)] = string(record.Value)

		case wal.OpDelete:
			delete(s.data, string(record.Key))
		}

		s.mu.Unlock()

		delete(s.pending, s.nextApplySeq)
		s.nextApplySeq++
	}
}