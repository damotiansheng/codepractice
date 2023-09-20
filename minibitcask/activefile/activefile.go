package minibitcask

import (
	"os"
	"strconv"
)

type DataFile struct {
	writeOffset	int64
	fid	uint32
	writeFile	*os.File
	readFile *os.File
	syncEnabled bool
	maxFileSize uint32
	dir string
}

func NewDataFile(dir string, fid uint32, maxFileSize uint32, syncEnabled bool) (*DataFile, error) {
	targetFileName := getActiveFilePath(dir, fid)
	f, err := os.OpenFile(targetFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	res := &DataFile{
		dir:	dir,
		writeOffset: 0,
		fid:         fid,
		writeFile:   f,
		syncEnabled: syncEnabled,
		maxFileSize: maxFileSize,
	}

	return res, nil
}

func (df *DataFile) Write(data []byte) (uint32, int64, error) {
	if df.writeOffset + int64(len(data)) > int64(df.maxFileSize) {
		df.fid++
		df.writeOffset = 0
		f, err:= os.OpenFile(getActiveFilePath(df.dir, df.fid), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return 0, 0, err
		}
		df.writeFile = f
	}

	_, err := df.writeFile.WriteAt(data, df.writeOffset)
	if err != nil {
		return 0, 0, err
	}

	if df.syncEnabled {
		err = df.writeFile.Sync()
		if err != nil {
			return 0, 0, err
		}
	}

	res := df.writeOffset
	df.writeOffset += int64(len(data))
	return df.fid, res, nil
}

func (df *DataFile) read(fid uint32, offset int64, valueSize uint32) ([]byte, error) {
	var err error
	df.readFile, err = os.OpenFile(getActiveFilePath(df.dir, fid), os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	res := make([]byte, valueSize)
	_, err = df.readFile.ReadAt(res, offset)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (df *DataFile) Close() error {
	if df.writeFile != nil {
		err := df.writeFile.Close()
		if err != nil {
			return err
		}
	}

	if df.readFile != nil {
		err := df.readFile.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

