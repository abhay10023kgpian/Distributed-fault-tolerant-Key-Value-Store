package wal

import "os"

type WAL struct {
	file *os.File
}

func Open (path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &WAL{file: file}, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}


type OpType byte

const (
	OpPut OpType = iota
	OpDelete
)

type Record struct {
	Op    OpType
	Key   []byte
	Value []byte
}

