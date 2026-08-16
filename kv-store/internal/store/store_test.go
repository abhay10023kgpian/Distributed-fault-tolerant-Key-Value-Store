package store

// import (
// 	"fmt"
// 	"sync"
// 	"testing"
// )

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

func TestConcurrentAccess(t *testing.T) {
	s := New()

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