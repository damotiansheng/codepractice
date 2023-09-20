package minibitcask

import (
	"encoding/binary"
	"time"
	"hash/crc32"
)

type Record struct {
	crc   uint32
	ts    uint64
	keySize uint32
	valueSize uint32
	key   []byte
	value []byte
}

type Hint struct {
	fid	string
	valueSize uint32
	valuePos  uint32
	ts	uint64
}

func NewRecord(key, value []byte) *Record {
	res := &Record{}
	res.key = key
	res.value = value
	res.keySize = uint32(len(key))
	res.valueSize = uint32(len(value))
	res.ts = uint64(time.Now().UnixMilli())
	res.crc = crc32.ChecksumIEEE(res.Encode()[4:])
	return res
}

func (r *Record) Size() uint32 {
	return 20 + r.keySize + r.valueSize
}

func (r *Record) Encode() []byte {
	res := make([]byte, r.Size())
	binary.LittleEndian.PutUint32(res[0:4], r.crc)
	binary.LittleEndian.PutUint64(res[4:12], r.ts)
	binary.LittleEndian.PutUint32(res[12:16], r.keySize)
	binary.LittleEndian.PutUint32(res[16:20], r.valueSize)
	copy(res[20:], r.key)
	copy(res[20+r.keySize:], r.value)
	return res
}

func Decode(data []byte) *Record {
	res := &Record{}
	res.crc = binary.LittleEndian.Uint32(data[0:4])
	res.ts = binary.LittleEndian.Uint64(data[4:12])
	res.keySize = binary.LittleEndian.Uint32(data[12:16])
	res.valueSize = binary.LittleEndian.Uint32(data[16:20])
	res.key = data[20:20+res.keySize]
	res.value = data[20+res.keySize:]
	return res
}



