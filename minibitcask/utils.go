package minibitcask

import "strconv"

func getActiveFilePath(dir string, fid uint32) string {
	targetFileName := dir + "/" + strconv.Itoa(int(fid)) + ".dat"
	return targetFileName
}

