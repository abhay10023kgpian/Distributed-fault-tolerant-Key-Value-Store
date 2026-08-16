package store

import "testing"

func BenchmarkSet(b *testing.B) {
	s := New()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set("key", "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", "value")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}