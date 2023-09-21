package minibitcask

import (
	"minibitcask/activefile"
	"minibitcask/utils"
	"sync"
)

type DB struct {
	data map[string]*Hint
	opt	*Options
	activeFile *activefile.ActiveFile
	rwLock *sync.RWMutex
}

func Open(opt *Options, ops ...Option) (*DB, error) {
	for _, op := range ops {
		op(opt)
	}

	maxFid := 0
	activeFile, err := activefile.NewActiveFile(opt.dir, uint32(maxFid), opt.maxActiveFileSize, opt.syncEnable)
	if err != nil {
		return nil, err
	}

	res := &DB{
		data: make(map[string]*Hint),
		opt: opt,
		activeFile: activeFile,
		rwLock:  &sync.RWMutex{},}

	return res, nil
}

func (db *DB) Close() error {
	return db.activeFile.Close()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.rwLock.RLock()
	defer db.rwLock.RUnlock()

	strKey := string(key)
	if _, ok := db.data[strKey]; !ok {
		return nil, ErrKeyNotFound
	}

	hint := db.data[strKey]
	targetPath := utils.GetActiveFilePath(db.opt.dir, hint.fid)
	resBytes, err := utils.Read(targetPath, int64(hint.valuePos), hint.valueSize)
	if err != nil {
		return nil, err
	}

	r := Decode(resBytes)
	if r.crc != hint.crc {
		return nil, ErrCrcNotMatch
	}

	return r.value, nil
}

func (db *DB) Put(key, value []byte) error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	r := NewRecord(key, value, TYPE_RECORD_PUT)
	rbytes := r.Encode()
	fid, valuePos, err := db.activeFile.Write(rbytes)
	if err != nil {
		return err
	}

	db.data[string(key)] = &Hint{fid: fid, valuePos: uint32(valuePos), valueSize:  uint32(len(rbytes)), ts: r.ts, crc: r.crc}
	return nil
}

func (db *DB) Delete(key []byte) error {
	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	if _, ok := db.data[string(key)]; !ok {
		return ErrKeyNotFound
	}

	r := NewRecord(key, nil, TYPE_RECORD_DELETE)
	_, _, err := db.activeFile.Write(r.Encode())
	if err != nil {
		return err
	}

	delete (db.data, string(key))
	return nil
}

