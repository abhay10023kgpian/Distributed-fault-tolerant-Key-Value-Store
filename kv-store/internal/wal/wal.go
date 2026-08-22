package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"io"
	"fmt"
)

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

func (w *WAL) Append(record Record) error {
	data, err := record.Encode()
	if err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil
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


