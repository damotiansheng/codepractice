package utils

import (
	"os"
	"strconv"
)

const (
	DATA_FILE_EXT = ".dat"
)

func GetActiveFilePath(dir string, fid uint32) string {
	return dir + "/" + strconv.Itoa(int(fid)) + DATA_FILE_EXT
}

func Read(path string, offset int64, valueSize uint32) ([]byte, error) {
	readFile, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	res := make([]byte, valueSize)
	_, err = readFile.ReadAt(res, offset)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func GetDirMaxFid(dir string) (uint32, error) {
    files, err := os.ReadDir(dir)
    if err != nil {
        return 0, err
    }

    // 获取最大的文件id
    maxFid := uint32(0)
    for _, file := range files {
        if file.IsDir() {
            continue
        }

		//a := "1.dat"
		// get prefix of a where rid of .dat

        fid, err := strconv.Atoi(file.Name())
        if err != nil {
            return 0, err
        }

        if fid > int(maxFid) {
            maxFid = uint32(fid)
        }
    }

    return maxFid, nil
}