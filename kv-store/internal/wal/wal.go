package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"io"
	"fmt"
	"sync"
)

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

type pendingWrite struct {
    record Record
    done   chan error
    seq    uint64
}

type file interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Seek(offset int64, whence int) (int64, error)
	Read([]byte) (int, error)
}

type WAL struct {
	file   file
	queue  chan pendingWrite
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool

	nextSeq uint64

	batchCount   int
	totalRecords int
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

	w := &WAL{
	file:  file,
	queue: make(chan pendingWrite, 1024),
	}

	w.start()

	return w, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return nil
	}

	w.closed = true
	close(w.queue)

	w.mu.Unlock()

	w.wg.Wait()

	return w.file.Close()
}

func (w *WAL) Append(record Record) (uint64,error) {
	request := pendingWrite{
		record: record,
		done:   make(chan error, 1),
	}

	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return 0, fmt.Errorf("WAL is closed")
	}

	w.nextSeq++
	request.seq = w.nextSeq

	w.queue <- request // triggers for loop in start() to encode the request

	w.mu.Unlock()

	if err := <-request.done; err != nil {
		return 0, err
	}

	return request.seq, nil
}

func (r Record) Encode() ([]byte, error) {
	var body bytes.Buffer

	keyLen := uint32(len(r.Key))
	valueLen := uint32(len(r.Value))

	// Body = Type + KeyLen + ValueLen + Key + Value
	body.WriteByte(byte(r.Op))

	if err := binary.Write(&body, binary.BigEndian, keyLen); err != nil {
		return nil, err
	}

	if err := binary.Write(&body, binary.BigEndian, valueLen); err != nil {
		return nil, err
	}

	body.Write(r.Key)
	body.Write(r.Value)

	// Checksum covers the entire body.
	checksum := crc32.ChecksumIEEE(body.Bytes())

	var record bytes.Buffer

	// Length = body + checksum.
	length := uint32(body.Len() + 4)

	if err := binary.Write(&record, binary.BigEndian, length); err != nil {
		return nil, err
	}

	record.Write(body.Bytes())

	if err := binary.Write(&record, binary.BigEndian, checksum); err != nil {
		return nil, err
	}

	return record.Bytes(), nil
}


func decodeRecord(data []byte) (Record, error) {
	if len(data) < 13 {
		return Record{}, fmt.Errorf("record too small")
	}

	op := OpType(data[0])

	keyLen := binary.BigEndian.Uint32(data[1:5])
	valueLen := binary.BigEndian.Uint32(data[5:9])

	expectedLen := 9 + int(keyLen) + int(valueLen) + 4

	if len(data) != expectedLen {
		return Record{}, fmt.Errorf("invalid record length")
	}

	keyStart := 9
	keyEnd := keyStart + int(keyLen)

	valueStart := keyEnd
	valueEnd := valueStart + int(valueLen)

	key := data[keyStart:keyEnd]
	value := data[valueStart:valueEnd]

	storedChecksum := binary.BigEndian.Uint32(data[valueEnd:])

	body := data[:valueEnd]
	calculatedChecksum := crc32.ChecksumIEEE(body)

	if storedChecksum != calculatedChecksum {
		return Record{}, fmt.Errorf("checksum mismatch")
	}

	return Record{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}


func (w *WAL) Replay() ([]Record, error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var records []Record

	for {
		var length uint32

		err := binary.Read(w.file, binary.BigEndian, &length)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		data := make([]byte, length)

		if _, err := io.ReadFull(w.file, data); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		record, err := decodeRecord(data)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}



func (w *WAL) start() {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		for first := range w.queue {
			batch := []pendingWrite{first}

			// Collect writes that are already waiting.
			for {
				select {
				case request := <-w.queue:
					batch = append(batch, request)
				default:
					goto commit
				}
			}

		commit:
			// Encode the entire batch first.
			w.mu.Lock()
			w.batchCount++
			w.totalRecords += len(batch)
			w.mu.Unlock()
			encoded := make([][]byte, len(batch))

			encodeErr := false

			for i, request := range batch {
				data, err := request.record.Encode()
				if err != nil {
					encodeErr = true
					break
				}

				encoded[i] = data
			}

			if encodeErr {
				err := fmt.Errorf("failed to encode WAL batch")

				for _, request := range batch {
					request.done <- err
				}

				continue
			}

			// Write every record.
			writeErr := false

			for _, data := range encoded {
				if _, err := w.file.Write(data); err != nil {
					writeErr = true
					break
				}
			}

			if writeErr {
				err := fmt.Errorf("failed to write WAL batch")

				for _, request := range batch {
					request.done <- err
				}

				continue
			}

			// ONE Sync for the entire batch.
			if err := w.file.Sync(); err != nil {
				for _, request := range batch {
					request.done <- err
				}

				continue
			}

			// Everything in the batch is durable.
			for _, request := range batch {
				request.done <- nil
			}
		}
	}()
}

func (w *WAL) BatchStats() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.batchCount, w.totalRecords
}