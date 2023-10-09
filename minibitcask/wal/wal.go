package wal

import (
	"encoding/binary"
	"hash/crc32"
)

type SegmentID = uint32

type Options struct {
	DirPath	string
	SegmentSize	int64
	SegmentFileExt	string
	SyncEnabled	bool
}

type WalPos interface {
	GetFileFid() uint32
	GetOffset() int64
	GetValueSize() int64
}

type WalReader interface {
	Close() error
	Next() ([]byte, WalPos, error)
}

type Wal interface {
	Open(options *Options) error
	Close() error
	// Write writes a data to the log.
	Write(data []byte) (WalPos, error)
	// Read reads a data from the log.
	Read(pos WalPos) ([]byte, error)
	OpenNewActiveSegment() error
	Sync() error
	NewWalReader(maxFid uint32) (WalReader, error)
}

type LogRecord struct {
	crc	uint32
	dataSize uint32
	data []byte
}

func NewLogRecord(data []byte) *LogRecord {
	return &LogRecord{
		crc:      crc32.ChecksumIEEE(data),
		dataSize: uint32(len(data)),
		data:     data,
	}
}

func (logRecord *LogRecord) Encode() []byte {
	buf := make([]byte, 8 + logRecord.dataSize)
	binary.BigEndian.PutUint32(buf[0:4], logRecord.crc)
	binary.BigEndian.PutUint32(buf[4:8], logRecord.dataSize)
	copy(buf[8:], logRecord.data)
	return buf
}

func Decode(data []byte) *LogRecord {
	logRecord := &LogRecord{}
	logRecord.crc = binary.BigEndian.Uint32(data[0:4])
	logRecord.dataSize = binary.BigEndian.Uint32(data[4:8])
	logRecord.data = data[8:]
	return logRecord
}

