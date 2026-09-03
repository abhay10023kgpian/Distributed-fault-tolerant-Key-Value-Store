package store

import (
	"fmt"
	"kv-store/internal/wal"
	"testing"
	"sync"
)

func TestConcurrentAccess(t *testing.T) {
	w, _ := wal.Open("test.wal")

	s, err := New(w)

	if err != nil {
		t.Fatal(err)
	}

	const workers = 100

	done := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		go func(i int) {
			key := "key"

			s.Set(key, "value")

			_, _ = s.Get(key)

			s.Delete(key)

			done <- true
		}(i)
	}

	for i := 0; i < workers; i++ {
		<-done
	}
}

func TestStorePersistence(t *testing.T) {
	w, err := wal.Open("test.wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(w)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Set("key", "value11"); err != nil {
		t.Fatal(err)
	}

	if err := s.Set("key2", "value2"); err != nil {
		t.Fatal(err)
	}

	if exists, err := s.Delete("key"); err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Fatal("key not founddd")
	}

	w.Close()

	w, err = wal.Open("test.wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err = New(w)
	if err != nil {
		t.Fatal(err)
	}

	value, exists := s.Get("key2")
	if !exists {
		t.Fatal("key not founddd")
	}

	fmt.Printf("value: %s\n", value)

}



func TestConcurrentSet(t *testing.T) {
	walPath := t.TempDir() + "/test.wal"
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(w)
	if err != nil {
		t.Fatal(err)
	}

	defer w.Close()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)

			if err := s.Set(key, value); err != nil {
				t.Errorf("Set failed for key %s: %v", key, err)
			}
		}(i)

	}

	wg.Wait()

	for i := 0; i < goroutines; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i) // Randomly change the expected value to simulate concurrent updates

		value, exists := s.Get(key)

		if !exists {
			t.Errorf("Get failed for key %s: not found", key)
		}
		if value != expectedValue {
			t.Errorf("Get failed for key %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}


func TestConcurrentGet(t *testing.T) {
	walPath := t.TempDir() + "/wal.log"
	w, err := wal.Open(walPath)

	if err != nil {
		t.Fatal(err)
	}

	defer w.Close()

	s, err := New(w)
	if err != nil {
		t.Fatal(err)
	}

	goroutines := 100

	for i := 0; i < goroutines; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)

		err := s.Set(key, value)
		if err != nil {
			t.Errorf("Set failed for key %s: %v", key, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key%d", i)
			expectedValue := fmt.Sprintf("value%d", i)

			value, exists := s.Get(key)

			if !exists {
				t.Errorf("Get failed for key %s: not found", key)
			}

			if value != expectedValue {
				t.Errorf("Get failed for key %s: expected %s, got %s", key, expectedValue, value)
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrentReadWrite(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	const writers = 50
	const readers = 50

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			if err := s.Set(key, value); err != nil {
				t.Errorf("Set failed: %v", err)
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			s.Get(key)
		}(i)
	}

	wg.Wait()
}


func TestConcurrentSetOrdering(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	const writers = 3

	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 1; i <= writers; i++ {
		value := fmt.Sprintf("value%d", i)

		go func(value string) {
			defer wg.Done()

			if err := s.Set("key", value); err != nil {
				t.Errorf("Set failed: %v", err)
			}
		}(value)
	}

	wg.Wait()

	value, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}

	t.Logf("final value: %s", value)
}


func TestApplyOrdered(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	record1 := wal.Record{
		Op:    wal.OpPut,
		Key:   []byte("key"),
		Value: []byte("value1"),
	}

	record2 := wal.Record{
		Op:    wal.OpPut,
		Key:   []byte("key"),
		Value: []byte("value2"),
	}

	record3 := wal.Record{
		Op:    wal.OpPut,
		Key:   []byte("key"),
		Value: []byte("value3"),
	}

	// Apply out of order.
	s.applyOrdered(3, record3)
	s.applyOrdered(1, record1)
	s.applyOrdered(2, record2)

	value, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "value3" {
		t.Fatalf("expected final value to be value3, got %q", value)
	}
}


func TestConcurrentSetPersistence(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}

	s, err := New(w)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	const writers = 100

	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			if err := s.Set(key, value); err != nil {
				t.Errorf("Set failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("close WAL failed: %v", err)
	}

	// Reopen the WAL to simulate a restart.
	w, err = wal.Open(path)
	if err != nil {
		t.Fatalf("reopen WAL failed: %v", err)
	}
	defer w.Close()

	recovered, err := New(w)
	if err != nil {
		t.Fatalf("recover store failed: %v", err)
	}

	for i := 0; i < writers; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)

		value, ok := recovered.Get(key)
		if !ok {
			t.Fatalf("missing key %q after recovery", key)
		}

		if value != expected {
			t.Fatalf(
				"key %q: expected %q, got %q",
				key,
				expected,
				value,
			)
		}
	}
}