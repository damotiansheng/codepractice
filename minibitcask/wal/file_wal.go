package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"minibitcask/utils"
	"os"
	"sort"
	"sync"
)

var (
	// ErrCrcNotMatch is returned when the key is found but the value is not valid.
	ErrCrcNotMatch = errors.New("crc not match")
)

const (
	SEGMENT_FILE_EXT	= ".SEG"
)

type FilePos struct {
	Fid	uint32
	Offset	int64
	ValueSize	int64
}

func (fp *FilePos) GetFileFid() uint32 {
	return fp.Fid
}

func (fp *FilePos) GetOffset() int64 {
	return fp.Offset
}

func (fp *FilePos) GetValueSize() int64 {
    return fp.ValueSize
}

type Segment struct {
	id	SegmentID
	fd	*os.File
	fid	uint32  // cur fid of file
	offset int64
}

type FileWal struct {
	options	*Options
	activeSegment	*Segment
	olderSegments   map[SegmentID]*Segment
	mu	sync.RWMutex
}

func OpenFileWal(options *Options) (Wal, error) {
	wal := &FileWal{
		options: options,
		olderSegments: make(map[SegmentID]*Segment),
	}
	err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return wal, nil
}

type FileWalReader struct {
	segments []*Segment
	curSegIdx int
}

func (fwr *FileWalReader) Close() error {
	for _, segment := range fwr.segments {
		err := segment.fd.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fwr *FileWalReader) Next() ([]byte, WalPos, error) {
    if fwr.curSegIdx >= len(fwr.segments) {
        return nil, nil, io.EOF
    }

	segment := fwr.segments[fwr.curSegIdx]

	// read header
	head := make([]byte, 8)
	_, err := segment.fd.ReadAt(head, segment.offset)
	if err == io.EOF {
		fwr.curSegIdx++
		return fwr.Next()
	}

	if err != nil {
		return nil, nil, err
	}

	// read data
	crc := binary.BigEndian.Uint32(head[:4])
	dataSize := binary.BigEndian.Uint32(head[4:8])
	data := make([]byte, dataSize)
	_, err = segment.fd.ReadAt(data, segment.offset + 8)
	if err != nil {
		return nil, nil, err
	}

	if crc != crc32.ChecksumIEEE(data) {
		return nil, nil, ErrCrcNotMatch
	}

	valueSize := int64(len(data) + 8)
	walPos := &FilePos{Fid: segment.fid, Offset: segment.offset, ValueSize: valueSize}

	// update offset
	segment.offset += valueSize

	return data, walPos, nil
}

func (wal *FileWal) NewWalReader(maxFid uint32) (WalReader, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	fileWalReader := &FileWalReader{}
	fileWalReader.curSegIdx = 0

	for fid, _ := range wal.olderSegments {
		if 0 == maxFid || fid <= maxFid {
			segment, err := wal.openSegment(fid, os.O_RDONLY)
			if err != nil {
				return nil, err
			}
			fileWalReader.segments = append(fileWalReader.segments, segment)
		}
	}

	if 0 == maxFid || wal.activeSegment.fid <= maxFid {
		segment, err := wal.openSegment(wal.activeSegment.fid, os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		fileWalReader.segments = append(fileWalReader.segments, segment)
	}

	// sort fileWalReader.segments by fid asc
	sort.Slice(fileWalReader.segments, func(i, j int) bool {
		return fileWalReader.segments[i].fid < fileWalReader.segments[j].fid
	})

	return fileWalReader, nil
}

func (wal *FileWal) openSegment(fid SegmentID, flag int) (*Segment, error) {
	segment := &Segment{id: fid, fid: fid, offset: 0}
	var err error
	segment.fd, err = os.OpenFile(utils.GetSegmentFilePath(wal.options.DirPath, fid, SEGMENT_FILE_EXT), flag, 0666)
	if err != nil {
		fmt.Println("open segment file error:", err)
		return nil, err
	}

	return segment, nil
}

func (wal *FileWal) Open(opt *Options) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// get max fid in current dir path save in wal.fid
	fids, err := utils.GetDataFiles(opt.DirPath, wal.options.SegmentFileExt)
	if err != nil {
		return err
	}

	if len(fids) == 0 {
		fids = append(fids, 0)
	}

	for i, fid := range fids {
		segment, err := wal.openSegment(fid, os.O_RDWR|os.O_CREATE)
		if err != nil {
			return err
		}

		if i != (len(fids) - 1) {
			wal.olderSegments[fid] = segment
		} else {
			offset, err := segment.fd.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}
			segment.offset = offset
			wal.activeSegment = segment
		}
	}

	return nil
}

func (wal *FileWal) OpenNewActiveSegment() error {
	// sync file
	err := wal.activeSegment.fd.Sync()
	if err != nil {
		return err
	}

	// open new segment file
	segment, err := wal.openSegment(wal.activeSegment.fid + 1, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return err
	}

	// rotate segment file
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment

	return nil
}

func (wal *FileWal) Close() error {
	// close file
	if err := wal.activeSegment.fd.Close(); err != nil {
		return err
	}

	for _, segment := range wal.olderSegments {
		if err := segment.fd.Close(); err != nil {
			return err
		}
	}

    return nil
}

func (wal *FileWal) isFull(data []byte) bool {
	return wal.activeSegment.offset + int64(len(data)) > wal.options.SegmentSize
}

func (wal *FileWal) Write(data []byte) (WalPos, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// generate logRecord: crc(4B) | length(4B) | data
	logRecordData := NewLogRecord(data).Encode()

	// rotate file if needed
	if wal.isFull(logRecordData) {
		err := wal.OpenNewActiveSegment()
		if err != nil {
			return nil, err
		}
	}

	// write logRecord data to file
	_, err := wal.activeSegment.fd.WriteAt(logRecordData, wal.activeSegment.offset)
	if err != nil {
		return nil, err
	}

	// sync data if syncEnabled is enabled
	if wal.options.SyncEnabled {
		err = wal.activeSegment.fd.Sync()
		if err != nil {
			return nil, err
		}
	}

	// get write file position and return
	filePos := &FilePos{Fid: wal.activeSegment.fid, Offset: wal.activeSegment.offset, ValueSize: int64(len(logRecordData))}

	// update write offset
	wal.activeSegment.offset += int64(len(logRecordData))

    return filePos, nil
}

func (wal *FileWal) Read(pos WalPos) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get target segment
	var segment *Segment
	if pos.GetFileFid() == wal.activeSegment.fid {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.GetFileFid()]
	}

	//  read logRecord data according to pos
	logRecordBytes := make([]byte, pos.GetValueSize())

	_, err := segment.fd.ReadAt(logRecordBytes, pos.GetOffset())
	if err != nil {
		return nil, err
	}

	// decode logRecord and return data
	logRecord := Decode(logRecordBytes)

	// check crc
	if logRecord.crc != crc32.ChecksumIEEE(logRecord.data) {
		return nil, ErrCrcNotMatch
	}

	return logRecord.data, nil
}

func (wal *FileWal) Sync() error {
    return wal.activeSegment.fd.Sync()
}
