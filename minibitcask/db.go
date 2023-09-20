package minibitcask

import (
	"sync"
)

type DB struct {
	data map[string]Hint
	opt	*Options
	rwLock *sync.RWMutex
}

func Open(opt *Options, ops ...Option) *DB {
	for _, op := range ops {
		op(opt)
	}

	return &DB{
		data: make(map[string]Hint),
		rwLock:  &sync.RWMutex{},
		opt: opt,}
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *DB) Put(key, value []byte) error {
	return nil
}

func (db *DB) Delete(key []byte) error {
	return nil
}


