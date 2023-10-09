package minibitcask

import (
	"io"
	"minibitcask/wal"
	"os"
	"sync"

	"github.com/xujiajun/utils/filesystem"
)

type DB struct {
	data	   map[string]wal.WalPos
	wal	       wal.Wal
	opt        *Options
	merge      *Merge
	rwLock     *sync.RWMutex
}

func Open(opt *Options, ops ...Option) (*DB, error) {
	for _, op := range ops {
		op(opt)
	}

	db := &DB{
		data:   make(map[string]wal.WalPos),
		opt:    opt,
		rwLock: &sync.RWMutex{},}

	// create dir
	if ok := filesystem.PathIsExist(db.opt.dir); !ok {
		if err := os.MkdirAll(db.opt.dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create data wal
	walOptions := &wal.Options{
		DirPath:        opt.dir,
		SegmentSize:    opt.maxActiveFileSize,
		SegmentFileExt: wal.SEGMENT_FILE_EXT,
		SyncEnabled: opt.syncEnable,
	}
	wal, err := wal.OpenFileWal(walOptions)
	if err != nil {
		return nil, err
	}
	db.wal = wal

	// build index
	if err := db.buildIndex(); err != nil {
		return nil, err
	}

	// start merge
	db.merge = NewMerge(db)
	db.merge.Start()

	return db, nil
}

func (db *DB) GetOpt() *Options {
	return db.opt
}

func (db *DB) buildIndex() error {
	// get wal reader
	reader, err := db.wal.NewWalReader(0)
	if err != nil {
		return err
	}

	// iterate all wal file and build index
	for {
		data, walPos, err := reader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		// decode data
		record := DecodeRecord(data)
		if record.GetFlag() != TYPE_RECORD_DELETE {
			db.data[string(record.key)] = walPos
		} else {
			delete(db.data, string(record.key))
		}
	}

	return nil
}

func (db *DB) Close() error {
	db.merge.Close()
	if err := db.wal.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) GetSize() int {
	return len(db.data)
}

func (db *DB) Merge() error {
	return db.merge.beginMerge()
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

	data, err := db.wal.Read(db.data[strKey])
	if err != nil {
		return nil, err
	}

	// Decode value
	r := DecodeRecord(data)

	return r.value, nil
}

func (db *DB) Rotate() error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()
	return db.wal.OpenNewActiveSegment()
}

func (db *DB) Put(key, value []byte) error {
	// Acquire read/write lock
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	// Create new record
	r := NewRecord(key, value, TYPE_RECORD_PUT)

	// Write record to wal
	walPos, err := db.wal.Write(r.EncodeRecord())
	if err  != nil {
		return err
	}

	// build index
	db.data[string(key)] = walPos

	return nil
}

func (db *DB) MergeRecord(data []byte, r *Record, walPos wal.WalPos) error {
    // Acquire read/write lock
    db.rwLock.Lock()
	defer db.rwLock.Unlock()

	strKey := string(r.key)
    // Check if key exists, only record in index is valid
    if _, ok := db.data[strKey]; !ok {
        return nil
    }

	indexWalPos := db.data[strKey]
	if indexWalPos.GetFileFid() != walPos.GetFileFid() || indexWalPos.GetOffset() != walPos.GetOffset() || indexWalPos.GetValueSize() != walPos.GetValueSize() {
		return nil
	}

	// Write record to wal
	walPos, err := db.wal.Write(data)
	if err != nil {
		return err
	}

	// update index
	db.data[strKey] = walPos

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

	// write wal log
	_, err := db.wal.Write(r.EncodeRecord())
	if err != nil {
		return err
	}

	// Delete key from data
	delete(db.data, string(key))

	return nil
}
