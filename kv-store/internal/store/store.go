package store

import (
	"sync"
	"kv-store/internal/wal"
)

type Store struct {
    mu   sync.RWMutex
    data map[string]string
    wal  *wal.WAL
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
		data: data,
		wal:  w,
	}, nil
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := wal.Record{
		Op: wal.OpPut,
		Key: []byte(key),
		Value: []byte(value),	
	}

	if err := s.wal.Append(record); err != nil {
		return err
	}

	s.data[key] = value
	return nil
}

func (s * Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Delete(key string)  (bool, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	record := wal.Record{
		Op: wal.OpDelete,
		Key: []byte(key),
	}

	if err := s.wal.Append(record); err != nil {
		return false, err
	}

	_, ok := s.data[key]
	
	if ok {
		delete(s.data, key)
	}

	return ok, nil
}