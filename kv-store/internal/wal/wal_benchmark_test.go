package wal

import "testing"

func BenchmarkWALAppend(b *testing.B) {
	path := b.TempDir() + "/wal.log"

	w, err := Open(path)
	if err != nil {
		b.Fatalf("open WAL failed: %v", err)
	}
	defer w.Close()

	record := Record{
		Op:    OpPut,
		Key:   []byte("name"),
		Value: []byte("Abhay"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := w.Append(record); err != nil {
			b.Fatalf("append record failed: %v", err)
		}
	}
}