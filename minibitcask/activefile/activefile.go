package minibitcask

import "os"

type DataFile struct {
	writeOffset	uint64
	fid	uint32
	file	*os.File
}

