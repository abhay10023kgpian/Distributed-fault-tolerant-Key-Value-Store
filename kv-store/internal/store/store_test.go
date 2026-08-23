package store

import (
	"fmt"

	
)

// func TestConcurrentSet(t *testing.T) {
// 	s := New()

// 	var wg sync.WaitGroup

// 	for i := 0; i < 1000; i++ {
// 		wg.Add(1)

// 		go func(i int) {
// 			defer wg.Done()

// 			key := fmt.Sprintf("key-%d", i)
// 			s.Set(key, "value")
// 		}(i)
// 	}

// 	wg.Wait()

// 	for i := 0; i < 1000; i++ {
// 		key := fmt.Sprintf("key-%d", i)

// 		_, exists := s.Get(key)

// 		if !exists {
// 			t.Fatalf("missing key: %s", key)
// 		}
// 	}
// }


import "testing"
import "kv-store/internal/wal"

func TestConcurrentAccess(t *testing.T) {
	w,_ := wal.Open("test.wal")
	
	s,err := New(w)

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
	w, err := wal.Open("test.wal"); if err != nil {
		t.Fatal(err)
	}

	s, err := New(w); if err != nil {
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

	w, err = wal.Open("test.wal"); if err != nil {
		t.Fatal(err)
	}

	s, err = New(w); if err != nil {
		t.Fatal(err)
	}

	value, exists := s.Get("key2")
	if !exists {
		t.Fatal("key not founddd")
	}

	fmt.Printf("value: %s\n", value)

}

