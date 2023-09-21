package minibitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	TYPE_RECORD_PUT uint16 = 0
	TYPE_RECORD_DELETE uint16 = 1
)

type Record struct {
	crc   uint32
	ts    uint64
	flag  uint16
	keySize uint32
	valueSize uint32
	key   []byte
	value []byte
}

type Hint struct {
	crc uint32
	fid	uint32
	valueSize uint32
	valuePos  uint32
	ts	uint64
}

func NewRecord(key, value []byte, recordType uint16) *Record {
	res := &Record{}
	res.key = key
	res.value = value
	res.keySize = uint32(len(key))
	res.valueSize = uint32(len(value))
	res.ts = uint64(time.Now().UnixMilli())
	res.flag = recordType
	res.crc = crc32.ChecksumIEEE(res.Encode()[4:])
	return res
}

func (r *Record) Size() uint32 {
	return 22 + r.keySize + r.valueSize
}

func (r *Record) Encode() []byte {
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

func Decode(data []byte) *Record {
	res := &Record{}
	res.crc = binary.LittleEndian.Uint32(data[0:4])
	res.ts = binary.LittleEndian.Uint64(data[4:12])
	res.flag = binary.LittleEndian.Uint16(data[12:14])
	res.keySize = binary.LittleEndian.Uint32(data[14:18])
	res.valueSize = binary.LittleEndian.Uint32(data[18:22])
	res.key = data[22:22+res.keySize]
	res.value = data[22+res.keySize:]
	return res
}



