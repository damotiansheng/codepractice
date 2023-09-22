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

	/*
	maxFid := 0
	activeFile, err := activefile.NewActiveFile(opt.dir, uint32(maxFid), opt.maxActiveFileSize, opt.syncEnable)
	if err != nil {
		return nil, err
	}*/

	db := &DB{
		data: make(map[string]*Hint),
		opt: opt,
		rwLock:  &sync.RWMutex{},}

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

	if len(fids) <= 0 {
		return nil
	}

	maxFid := fids[len(fids) - 1]
    for _, fid := range fids {
        // get data file path
        dataFilePath := utils.GetActiveFilePath(db.opt.dir, fid)

        // open data file
		fileLen, err := db.parseDataFile(dataFilePath);
		if err != nil {
			return err
		}

		if fid == maxFid {
			db.activeFile, err = activefile.NewActiveFile(db.opt.dir, fid, fileLen, db.opt.maxActiveFileSize, db.opt.syncEnable)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (db *DB) parseDataFile(dataFilePath string) (len int64, err error) {
	var offset int64

    return offset, nil
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

	r := NewRecord(key, []byte(""), TYPE_RECORD_DELETE)
	_, _, err := db.activeFile.Write(r.Encode())
	if err != nil {
		return err
	}

	delete (db.data, string(key))
	return nil
}
