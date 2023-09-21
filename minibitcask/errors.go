package minibitcask

import "errors"

var (
	// ErrKeyNotFound is returned when a key is not found in the database.
	ErrKeyNotFound = errors.New("key not found")

	// ErrCrcNotMatch is returned when the key is found but the value is not valid.
	ErrCrcNotMatch = errors.New("crc not match")
)
