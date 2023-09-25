package minibitcask

import (
	"encoding/binary"
	"io"
	"minibitcask/activefile"
	"minibitcask/utils"
	"os"
	"sync"

	"github.com/xujiajun/utils/filesystem"
)

type DB struct {
	data       map[string]*Hint
	opt        *Options
	activeFile *activefile.ActiveFile
	rwLock     *sync.RWMutex
}

func Open(opt *Options, ops ...Option) (*DB, error) {
	for _, op := range ops {
		op(opt)
	}

	db := &DB{
		data:   make(map[string]*Hint),
		opt:    opt,
		rwLock: &sync.RWMutex{},}

	// create dir
	if ok := filesystem.PathIsExist(db.opt.dir); !ok {
		if err := os.MkdirAll(db.opt.dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.buildIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) buildIndex() error {
	// get all data file id list in db.opt.dir
	fids, err := utils.GetDataFiles(db.opt.dir, utils.DATA_FILE_EXT)
	if err != nil {
		return err
	}

	var activeFilefid uint32
	var writeOffset int64

	for _, fid := range fids {
		// open data file
		fileLen, err := db.parseDataFile(fid);
		if err != nil {
			return err
		}

		if fid == fids[len(fids)-1] {
			activeFilefid = fid
			writeOffset =  fileLen
		}
	}

	db.activeFile, err = activefile.NewActiveFile(db.opt.dir, activeFilefid, writeOffset, db.opt.maxActiveFileSize, db.opt.syncEnable)
	if err != nil {
		return err
	}

	return err
}

// parseDataFile reads and parses a data file with the given file ID.
// It returns the length of the file and any error encountered.
func (db *DB) parseDataFile(fid uint32) (fileLen int64, err error) {
	// Get the path of the active file
	path := utils.GetActiveFilePath(db.opt.dir, fid)

	var offset int64
	// Open the file for reading
	readFile, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return offset, err
	}

	// Read and parse each record in the file
	for {
		// Read the record head to get the key and value size
		res := make([]byte, RECORD_HEAD_SIZE)
		_, err = readFile.ReadAt(res, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return offset, err
		}
		// Extract the key size and value size from the record head
		var keySize uint32
		var valueSize uint32
		keySize = binary.LittleEndian.Uint32(res[14:18])
		valueSize = binary.LittleEndian.Uint32(res[18:22])

		// Calculate the record length
		recordLen := uint32(RECORD_HEAD_SIZE) + keySize + valueSize
		recordBytes := make([]byte, recordLen)
		// Read the record
		_, err := readFile.ReadAt(recordBytes, offset)
		if err != nil {
			return 0, err
		}

		// Decode the record
		record := Decode(recordBytes)

		// ignore deleted records
		if record.flag == TYPE_RECORD_DELETE {
			offset += int64(recordLen)
			continue
		}

		// Build the index for the record
		hint := &Hint{
			fid:        fid,
			crc:        record.crc,
			valueSize:  recordLen,
			valuePos:   uint32(offset),
			ts:         record.ts,
		}
		db.data[string(record.key)] = hint
		// Update the offset
		offset += int64(recordLen)
	}

	return offset, nil
}

func (db *DB) Close() error {
	return db.activeFile.Close()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// Acquire read lock
	db.rwLock.RLock()
	defer db.rwLock.RUnlock()

	// Check if key exists
	strKey := string(key)
	if _, ok := db.data[strKey]; !ok {
		return nil, ErrKeyNotFound
	}

	// Get hint
	hint := db.data[strKey]
	// Get active file path
	targetPath := utils.GetActiveFilePath(db.opt.dir, hint.fid)
	// Read value from active file
	resBytes, err := utils.Read(targetPath, int64(hint.valuePos), hint.valueSize)
	if err != nil {
		return nil, err
	}

	// Decode value
	r := Decode(resBytes)

	// Check crc
	if r.crc != hint.crc {
		return nil, ErrCrcNotMatch
	}

	return r.value, nil
}

func (db *DB) Put(key, value []byte) error {
	// Acquire read/write lock
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	// Create new record
	r := NewRecord(key, value, TYPE_RECORD_PUT)
	rbytes := r.Encode()
	// Write record to active file
	fid, valuePos, err := db.activeFile.Write(rbytes)
	if err != nil {
		return err
	}

	// Update hint
	db.data[string(key)] = &Hint{fid: fid, valuePos: uint32(valuePos), valueSize: uint32(len(rbytes)), ts: r.ts, crc: r.crc}
	return nil
}

func (db *DB) Delete(key []byte) error {
	// Acquire read/write lock
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	// Check if key exists
	if _, ok := db.data[string(key)]; !ok {
		return ErrKeyNotFound
	}

	// Create new record
	r := NewRecord(key, []byte(""), TYPE_RECORD_DELETE)
	_, _, err := db.activeFile.Write(r.Encode())
	if err != nil {
		return err
	}

	// Delete key from data
	delete(db.data, string(key))
	return nil
}