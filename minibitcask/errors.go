package minibitcask

import "errors"

var (
	// ErrKeyNotFound is returned when a key is not found in the database.
	ErrKeyNotFound = errors.New("key not found")
)
