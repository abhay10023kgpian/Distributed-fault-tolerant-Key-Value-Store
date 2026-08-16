package store

// import "testing"

// func TestConcurrentAccess(t *testing.T) {
// 	s := New()

// 	const workers = 100

// 	done := make(chan bool, workers)

// 	for i := 0; i < workers; i++ {
// 		go func(i int) {
// 			key := "key"

// 			s.Set(key, "value")

// 			_, _ = s.Get(key)

// 			s.Delete(key)

// 			done <- true
// 		}(i)
// 	}

// 	for i := 0; i < workers; i++ {
// 		<-done
// 	}
// }