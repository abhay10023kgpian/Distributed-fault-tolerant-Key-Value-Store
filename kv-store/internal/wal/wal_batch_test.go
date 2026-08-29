package wal

import (
	"sync"
	"testing"
)

type mockFile struct {
	mu         sync.Mutex
	writeCount int
	syncCount  int
}

func (m *mockFile) Write(data []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeCount++
	return len(data), nil
}

func (m *mockFile) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.syncCount++
	return nil
}

func (m *mockFile) Close() error {
	return nil
}

func (m *mockFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (m *mockFile) Read(data []byte) (int, error) {
	return 0, nil
}

func TestWALGroupCommit(t *testing.T) {
	mock := &mockFile{}
	w := &WAL{
		file:  mock,
		queue: make(chan pendingWrite, 1024),
	}

	w.start()

	const writers = 10

	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()

			record := Record{
				Op:    OpPut,
				Key:   []byte("key"),
				Value: []byte("value"),
			}

			request := pendingWrite{
				record: record,
				done:   make(chan error, 1),
			}

			w.queue <- request

			if err := <-request.done; err != nil {
				t.Errorf("append failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	close(w.queue)
	w.wg.Wait()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if mock.writeCount != writers {
		t.Fatalf(
			"expected %d writes, got %d",
			writers,
			mock.writeCount,
		)
	}

	if mock.syncCount >= writers {
		t.Fatalf(
			"expected fewer Sync calls than writes, got %d Syncs for %d writes",
			mock.syncCount,
			writers,
		)
	}
}
