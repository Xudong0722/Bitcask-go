package main

import (
	bitcask "Bitcask_go"
	cfg "Bitcask_go/config"
	"fmt"
)

func main() {
	opts := cfg.DefaultOptions
	opts.DataDir = "/tmp/bitcask-go"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

}
