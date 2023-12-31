package minibitcask

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"
)

const (
	TYPE_RECORD_PUT    uint16 = 0
	TYPE_RECORD_DELETE uint16 = 1
	RECORD_HEAD_SIZE   uint16 = 22
)

type Record struct {
	crc       uint32
	ts        uint64
	flag      uint16
	keySize   uint32
	valueSize uint32
	key       []byte
	value     []byte
}

type Hint struct {
	crc       uint32
	fid       uint32
	valueSize uint32
	valuePos  uint32
	ts        uint64
}

type HintRecord struct {
	keySize   uint32
	hint      *Hint
	key       []byte
}

func NewRecord(key, value []byte, recordType uint16) *Record {
	res := &Record{}
	res.key = key
	res.value = value
	res.keySize = uint32(len(key))
	res.valueSize = uint32(len(value))
	res.ts = uint64(time.Now().UnixMilli())
	res.flag = recordType
	res.crc = crc32.ChecksumIEEE(res.EncodeRecord()[4:])
	return res
}

func (r *Record) Size() uint32 {
	return 22 + r.keySize + r.valueSize
}

func (r *Record) GetFlag() uint16 {
	return r.flag
}

func (r *Record) EncodeRecord() []byte {
	res := make([]byte, r.Size())
	binary.LittleEndian.PutUint32(res[0:4], r.crc)
	binary.LittleEndian.PutUint64(res[4:12], r.ts)
	binary.LittleEndian.PutUint16(res[12:14], r.flag)
	binary.LittleEndian.PutUint32(res[14:18], r.keySize)
	binary.LittleEndian.PutUint32(res[18:22], r.valueSize)
	copy(res[22:], r.key)
	copy(res[22+r.keySize:], r.value)
	return res
}

func DecodeRecord(data []byte) *Record {
	res := &Record{}
	res.crc = binary.LittleEndian.Uint32(data[0:4])
	res.ts = binary.LittleEndian.Uint64(data[4:12])
	res.flag = binary.LittleEndian.Uint16(data[12:14])
	res.keySize = binary.LittleEndian.Uint32(data[14:18])
	res.valueSize = binary.LittleEndian.Uint32(data[18:22])
	res.key = data[22 : 22+res.keySize]
	res.value = data[22+res.keySize:]
	return res
}

func ReadRecord(readFile *os.File, offset int64) (*Record, error) {
	res := make([]byte, RECORD_HEAD_SIZE)
	_, err := readFile.ReadAt(res, offset)
	if err != nil {
		return nil, err
	}

	var keySize uint32
	var valueSize uint32
	keySize = binary.LittleEndian.Uint32(res[14:18])
	valueSize = binary.LittleEndian.Uint32(res[18:22])

	// Calculate the record length
	recordLen := uint32(RECORD_HEAD_SIZE) + keySize + valueSize
	recordBytes := make([]byte, recordLen)
	// Read the record
	_, err = readFile.ReadAt(recordBytes, offset)
	if err != nil {
		return nil, err
	}

	return DecodeRecord(recordBytes), err
}