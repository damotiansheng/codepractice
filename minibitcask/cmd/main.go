/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package main

import (
	"fmt"
	"minibitcask"
)

func main() {
	db, err := minibitcask.Open(minibitcask.DefaultOptions, minibitcask.WithDir("./"), minibitcask.WithSyncEnable(false), minibitcask.WithMaxActiveFileSize(1024 * 1024 * 1))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	n := 10000
	// put添加数据
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		err = db.Put(key, value)
		if  err != nil {
			panic(err)
		}
	}

	fmt.Println("finish put ", n, " items")

	// Get获取数据
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		dbValue, err := db.Get(key)
		if  err != nil {
			panic(err)
		}
		if string(value) != string(dbValue) {
			panic("value not equal")
		}
	}

	fmt.Println("finish get ", n, " items")

	// Delete删除数据
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		err := db.Delete(key)
		if  err != nil {
			panic(err)
		}
	}

	// merge data
	if err := db.Merge(); err != nil {
		panic(err)
	}
}
