package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/util"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(config.DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetTestKey(10), util.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(config.DefaultIteratorOptions)
	defer iterator.Close()

	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	t.Log(string(iterator.Key()))
	assert.Equal(t, util.GetTestKey(10), iterator.Key())
	val, _ := iterator.Value()
	t.Log(string(val))
	assert.Equal(t, util.GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("adjs"), util.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bkljkl"), util.RandomValue(12))
	assert.Nil(t, err)
	err = db.Put([]byte("test"), util.RandomValue(14))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(10), util.RandomValue(16))
	assert.Nil(t, err)

	//1. 正向迭代器
	iterator1 := db.NewIterator(config.DefaultIteratorOptions)
	defer iterator1.Close()

	assert.NotNil(t, iterator1)
	assert.Equal(t, true, iterator1.Valid())

	// 正向遍历
	for iterator1.Rewind(); iterator1.Valid(); iterator1.Next() {
		// t.Log("Key:", string(iterator1.Key()))
		val, _ := iterator1.Value()
		// t.Log("Value:", string(val))
		assert.NotNil(t, val)
		assert.NotNil(t, iterator1.Key())
	}

	//正向Seek
	iterator1.Rewind()
	for iterator1.Seek([]byte("c")); iterator1.Valid(); iterator1.Next() {
		val, _ := iterator1.Value()
		// t.Log(string(iterator1.Key()))
		// t.Log(string(val))
		assert.NotNil(t, val)
		assert.NotNil(t, iterator1.Key())
	}

	//2. 反向迭代器
	ops := config.DefaultIteratorOptions
	ops.Reverse = true

	iterator2 := db.NewIterator(ops)
	defer iterator2.Close()

	assert.NotNil(t, iterator2)
	assert.Equal(t, true, iterator2.Valid())

	// 反向遍历
	for iterator2.Rewind(); iterator2.Valid(); iterator2.Next() {
		// t.Log("Key:", string(iterator2.Key()))
		val, _ := iterator2.Value()
		// t.Log("Value:", string(val))
		assert.NotNil(t, val)
		assert.NotNil(t, iterator2.Key())
	}

	//反向Seek
	iterator2.Rewind()
	for iterator2.Seek([]byte("c")); iterator2.Valid(); iterator2.Next() {
		val, _ := iterator2.Value()
		// t.Log(string(iterator2.Key()))
		// t.Log(string(val))
		assert.NotNil(t, val)
		assert.NotNil(t, iterator2.Key())
	}

	//3.指定前缀的迭代器
	ops.Prefix = []byte("b")
	ops.Reverse = false

	iterator3 := db.NewIterator(ops)
	defer iterator3.Close()

	assert.NotNil(t, iterator3)
	assert.Equal(t, true, iterator3.Valid())

	for iterator3.Rewind(); iterator3.Valid(); iterator3.Next() {
		//t.Log("Key:", string(iterator3.Key()))
		val, _ := iterator3.Value()
		//t.Log("Value:", string(val))
		assert.NotNil(t, val)
		assert.NotNil(t, iterator3.Key())
	}
}
