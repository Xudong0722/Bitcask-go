package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/util"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		for _, of := range db.olderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.configuration.DataDir)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DataDir = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1.正常 Put 一条数据
	err = db.Put(util.GetTestKey(1), util.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(util.GetTestKey(1), util.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(util.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, util.RandomValue(24))
	assert.Equal(t, util.ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(util.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(util.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	if db.activeFile != nil {
		_ = db.activeFile.Close()
	}
	for _, of := range db.olderFiles {
		if of != nil {
			_ = of.Close()
		}
	}

	// 重启数据库
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := util.RandomValue(128)
	err = db2.Put(util.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(util.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(util.GetTestKey(11), util.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, util.ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(util.GetTestKey(22), util.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(22), util.RandomValue(24))
	assert.Nil(t, err)
	val3, err := db.Get(util.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(util.GetTestKey(33), util.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(util.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(util.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, util.ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(util.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db2)
	val6, err := db2.Get(util.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(util.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(util.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, util.ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DataDir = dir
	opts.DataFileMaxSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(util.GetTestKey(11), util.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(util.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(util.GetTestKey(11))
	assert.Equal(t, util.ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, util.ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(util.GetTestKey(22), util.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(util.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(util.GetTestKey(22), util.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(util.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db2)
	_, err = db2.Get(util.GetTestKey(11))
	assert.Equal(t, util.ErrKeyNotFound, err)

	val2, err := db2.Get(util.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-keys")
	opts.DataDir = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(util.GetTestKey(11), util.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(util.GetTestKey(22), util.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(33), util.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(44), util.RandomValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
		//t.Log(string(k))
	}
}

func TestDB_Fold(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-fold")
	opts.DataDir = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetTestKey(11), util.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(22), util.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(33), util.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(util.GetTestKey(44), util.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		//t.Log(string(key))
		//t.Log(string(value))
		// if bytes.Compare(key, util.GetTestKey(33)) == 0 {
		// 	return false
		// }
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DataDir = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetTestKey(11), util.RandomValue(20))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-sync")
	opts.DataDir = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(util.GetTestKey(11), util.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

// func TestDB_FileLock(t *testing.T) {
// 	opts := config.DefaultOptions
// 	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
// 	opts.DirPath = dir
// 	db, err := Open(opts)
// 	defer destroyDB(db)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, db)

// 	_, err = Open(opts)
// 	assert.Equal(t, ErrDatabaseIsUsing, err)

// 	err = db.Close()
// 	assert.Nil(t, err)

// 	db2, err := Open(opts)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, db2)
// 	err = db2.Close()
// 	assert.Nil(t, err)
// }

// func TestDB_Stat(t *testing.T) {
// 	opts := config.DefaultOptions
// 	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
// 	opts.DirPath = dir
// 	db, err := Open(opts)
// 	defer destroyDB(db)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, db)

// 	for i := 100; i < 10000; i++ {
// 		err := db.Put(util.GetTestKey(i), util.RandomValue(128))
// 		assert.Nil(t, err)
// 	}
// 	for i := 100; i < 1000; i++ {
// 		err := db.Delete(util.GetTestKey(i))
// 		assert.Nil(t, err)
// 	}
// 	for i := 2000; i < 5000; i++ {
// 		err := db.Put(util.GetTestKey(i), util.RandomValue(128))
// 		assert.Nil(t, err)
// 	}

// 	stat := db.Stat()
// 	assert.NotNil(t, stat)
// }

// func TestDB_Backup(t *testing.T) {
// 	opts := config.DefaultOptions
// 	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
// 	opts.DirPath = dir
// 	db, err := Open(opts)
// 	defer destroyDB(db)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, db)

// 	for i := 1; i < 1000000; i++ {
// 		err := db.Put(util.GetTestKey(i), util.RandomValue(128))
// 		assert.Nil(t, err)
// 	}

// 	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
// 	err = db.Backup(backupDir)
// 	assert.Nil(t, err)

// 	opts1 := config.DefaultOptions
// 	opts1.DirPath = backupDir
// 	db2, err := Open(opts1)
// 	defer destroyDB(db2)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, db2)
// }

//func TestDB_OpenMMap(t *testing.T) {
//	opts := config.DefaultOptions
//	opts.DirPath = "/tmp/bitcask-go"
//	opts.MMapAtStartup = false
//
//	now := time.Now()
//	db, err := Open(opts)
//	t.Log("open time ", time.Since(now))
//
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//}
