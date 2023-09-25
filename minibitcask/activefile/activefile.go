package activefile

import (
	"minibitcask/utils"
	"os"
)

type ActiveFile struct {
	writeOffset	int64
	fid	uint32
	writeFile	*os.File
	syncEnabled bool
	maxFileSize uint32
	dir string
}

func NewActiveFile(dir string, fid uint32, writeOffset int64, maxFileSize uint32, syncEnabled bool) (*ActiveFile, error) {
	targetFileName := utils.GetActiveFilePath(dir, fid)

	f, err := os.OpenFile(targetFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	res := &ActiveFile{
		dir:	dir,
		writeOffset: writeOffset,
		fid:         fid,
		writeFile:   f,
		syncEnabled: syncEnabled,
		maxFileSize: maxFileSize,
	}

	return res, nil
}

func (df *ActiveFile) Write(data []byte) (uint32, int64, error) {
	// Check if the write offset + the length of the data is greater than the max file size
	if df.writeOffset + int64(len(data)) > int64(df.maxFileSize) {
		// Increment the file id
		df.fid++
		// Reset the write offset to 0
		df.writeOffset = 0
		// Open the file for writing
		f, err:= os.OpenFile(utils.GetActiveFilePath(df.dir, df.fid), os.O_RDWR|os.O_CREATE, 0666)
		if err!= nil {
			return 0, 0, err
		}
		// Set the write file to the new file
		df.writeFile = f
	}

	// Write the data to the file
	_, err := df.writeFile.WriteAt(data, df.writeOffset)
	if err!= nil {
		return 0, 0, err
	}

	// Check if the sync flag is enabled
	if df.syncEnabled {
		// Sync the file
		err = df.writeFile.Sync()
		if err!= nil {
			return 0, 0, err
		}
	}

	// Update the write offset
	res := df.writeOffset
	df.writeOffset += int64(len(data))
	return df.fid, res, nil
}

func (df *ActiveFile) Close() error {
	return df.writeFile.Close()
}
