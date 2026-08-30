package store

import (
	"fmt"
	"kv-store/internal/wal"
	"math/rand"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	path := b.TempDir() + "/wal.log"
	w, err := wal.Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}	

	defer w.Close()
	
	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}


	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := s.Set(key, "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	path := b.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	if err := s.Set("key", "value"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkConcurrentSet(b *testing.B) {
	path := b.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			if err := s.Set(key, "value"); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	b.StopTimer()

	batches, records := w.BatchStats()

	avgBatchSize := float64(records) / float64(batches)

	b.Logf("records: %d", records)
	b.Logf("batches: %d", batches)
	b.Logf("average batch size: %.2f", avgBatchSize)
}

func BenchmarkMixedReadWrite(b *testing.B) {
	path := b.TempDir() + "/wal.log"

	w, err := wal.Open(path)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	if err := s.Set("key", "value"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if rand.Intn(10) < 8 {
				s.Get("key")
			} else {
				if err := s.Set("key", "value"); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}