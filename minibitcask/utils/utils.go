package utils

import (
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	HINT_FILE_EXT = ".hint"
)

func GetSegmentFilePath(dir string, fid uint32, fileExt string) string {
	return dir + "/" + strconv.Itoa(int(fid)) + fileExt
}

func GetHintFilePath(dir string, fid uint32) string {
    return dir + "/" + strconv.Itoa(int(fid)) + HINT_FILE_EXT
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

func GetDataFiles(dir string, suffix string) ([]uint32, error) {
	files, _ := ioutil.ReadDir(dir)
	if len(files) == 0 {
		return []uint32{}, nil
	}

	var fileIds []uint32
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), suffix) {
			continue
		}
		filename := file.Name()[:len(file.Name())-len(suffix)]
		fileId, err := strconv.Atoi(filename)
		if err != nil {
			return nil, err
		}

		fileIds = append(fileIds, uint32(fileId))
	}

	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})

	return fileIds, nil
}
