package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/util"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//写数据之后没有提交的场景
	wb := db.NewWriteBatch(config.DefaultWriteBatchOptions)
	err = wb.Put(util.GetTestKey(1), util.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(util.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(util.GetTestKey(1))
	assert.NotNil(t, err)

	//正常提交
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(util.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	//删除有效的数据
	wb2 := db.NewWriteBatch(config.DefaultWriteBatchOptions)
	err = wb2.Delete(util.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(util.GetTestKey(1))
	assert.Nil(t, val2)
	assert.NotNil(t, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetTestKey(1), util.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(config.DefaultWriteBatchOptions)
	err = wb.Put(util.GetTestKey(2), util.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(util.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(util.GetTestKey(11), util.RandomValue(11))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	//重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(util.GetTestKey(1))
	assert.Equal(t, util.ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := config.DefaultOptions
	// dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	dir := "/tmp/bitcask-go-batch-5"
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// wbOpts := config.DefaultWriteBatchOptions
	// wbOpts.MaxBatchNum = 1000000
	// wb := db.NewWriteBatch(wbOpts)
	// for i := 0; i < 500000; i++ {
	// 	err := wb.Put(util.GetTestKey(i), util.RandomValue(1000))
	// 	assert.Nil(t, err)
	// }

	// err = wb.Commit()
	// assert.Nil(t, err)

	keys := db.ListKeys()
	t.Log(len(keys))
}
