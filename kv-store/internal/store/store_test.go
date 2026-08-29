package store

import (
	"fmt"
	"kv-store/internal/wal"
	"testing"
	"sync"
)

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
