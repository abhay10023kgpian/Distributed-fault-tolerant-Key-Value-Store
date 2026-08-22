package wal

import "testing"

func TestRecordEncode(t *testing.T) {
	record := Record{
		Op:    OpPut,
		Key:   []byte("name"),
		Value: []byte("Abhay"),
	}

	data, err := record.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("encoded record is empty")
	}
}

func TestWALAppend(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}
	defer w.Close()

	record := Record{
		Op:    OpPut,
		Key:   []byte("name"),
		Value: []byte("Abhay"),
	}

	if err := w.Append(record); err != nil {
		t.Fatalf("append failed: %v", err)
	}
}


func TestWALReplay(t *testing.T) {
	path := t.TempDir() + "/wal.log"

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open WAL failed: %v", err)
	}

	records := []Record{
		{
			Op:    OpPut,
			Key:   []byte("name"),
			Value: []byte("Abhay"),
		},
		{
			Op:    OpPut,
			Key:   []byte("age"),
			Value: []byte("21"),
		},
		{
			Op:    OpDelete,
			Key:   []byte("name"),
		},
	}

	for _, record := range records {
		if err := w.Append(record); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen the same WAL file, simulating a server restart.
	w, err = Open(path)
	if err != nil {
		t.Fatalf("reopen WAL failed: %v", err)
	}
	defer w.Close()

	replayed, err := w.Replay()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if len(replayed) != len(records) {
		t.Fatalf(
			"expected %d records, got %d",
			len(records),
			len(replayed),
		)
	}

	for i := range records {
		if replayed[i].Op != records[i].Op {
			t.Fatalf("record %d: operation mismatch", i)
		}

		if string(replayed[i].Key) != string(records[i].Key) {
			t.Fatalf("record %d: key mismatch", i)
		}

		if string(replayed[i].Value) != string(records[i].Value) {
			t.Fatalf("record %d: value mismatch", i)
		}
	}
}