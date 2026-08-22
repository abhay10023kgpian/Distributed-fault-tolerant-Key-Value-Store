package store

import "testing"
import "kv-store/internal/wal"
func BenchmarkSet(b *testing.B) {
	w, err := wal.Open("test.wal")
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}	
	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set("key", "value")
	}
}

func BenchmarkGet(b *testing.B) {
	w, err := wal.Open("test.wal")
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	s, err := New(w)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	s.Set("key", "value")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}