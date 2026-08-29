package wal

import (
"testing"
)

func BenchmarkWriteOnly(b *testing.B) {
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

	
	data, err := record.Encode()
	if err != nil {
		b.Fatalf("encode record failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		if _, err := w.file.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}