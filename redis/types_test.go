package redis

import (
	"Bitcask_go/config"
	"Bitcask_go/util"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisData_Get(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DataDir = dir
	rdb, err := NewRedisDB(opts)
	assert.Nil(t, err)

	err = rdb.Set(util.GetTestKey(1), 0, util.RandomValue(100))
	assert.Nil(t, err)
	err = rdb.Set(util.GetTestKey(2), time.Second*5, util.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rdb.Get(util.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rdb.Get(util.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	time.Sleep(time.Second * 6)
	//key-2 expired
	val3, err := rdb.Get(util.GetTestKey(2))
	assert.Equal(t, ErrKeyIsExpired, err)
	assert.Nil(t, val3)
}

func TestRedisData_Del_Type(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DataDir = dir
	rdb, err := NewRedisDB(opts)
	assert.Nil(t, err)

	err = rdb.Del(util.GetTestKey(11))
	assert.Nil(t, err)

	err = rdb.Set(util.GetTestKey(1), 0, util.RandomValue(100))
	assert.Nil(t, err)

	typ := rdb.Type(util.GetTestKey(1))
	assert.NotNil(t, typ)
	assert.Equal(t, RString, typ)

	err = rdb.Del(util.GetTestKey(1))
	assert.Nil(t, err)

	val1, err := rdb.Get(util.GetTestKey(1))
	t.Log(err)
	assert.Nil(t, val1)
	assert.NotNil(t, err)

}

func TestRedisData_Hash(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hash")
	opts.DataDir = dir
	rdb, err := NewRedisDB(opts)
	assert.Nil(t, err)

	ok1, err := rdb.HSet(util.GetTestKey(1), []byte("field1"), util.RandomValue(100))
	assert.Equal(t, ok1, true)
	assert.Nil(t, err)

	v1 := util.RandomValue(100)
	ok2, err := rdb.HSet(util.GetTestKey(1), []byte("field1"), v1)
	assert.Equal(t, ok2, false)
	assert.Nil(t, err)

	v2 := util.RandomValue(101)
	ok3, err := rdb.HSet(util.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.Equal(t, ok3, true)

	val1, err := rdb.HGet(util.GetTestKey(1), []byte("field1"))
	assert.Equal(t, val1, v1)
	assert.Nil(t, err)
	val2, err := rdb.HGet(util.GetTestKey(1), []byte("field2"))
	assert.Equal(t, val2, v2)
	assert.Nil(t, err)
	val3, err := rdb.HGet(util.GetTestKey(1), []byte("field not exist"))
	assert.NotNil(t, err)
	assert.Nil(t, val3)

	del1, err := rdb.HDel(util.GetTestKey(2), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	del2, err := rdb.HDel(util.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)

	val4, err := rdb.HGet(util.GetTestKey(1), []byte("field1"))
	assert.Nil(t, val4)
	assert.NotNil(t, err)
}

func TestRedisData_Set(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-set")
	opts.DataDir = dir
	rdb, err := NewRedisDB(opts)
	assert.Nil(t, err)

	ok, err := rdb.SAdd(util.GetTestKey(2), []byte("test-2"))
	assert.Nil(t, err)
	assert.Equal(t, ok, true)

	ok2, err := rdb.SAdd(util.GetTestKey(2), []byte("test-2"))
	assert.Nil(t, err)
	assert.Equal(t, ok2, false)

	is1, err := rdb.SIsMember(util.GetTestKey(2), []byte("test-2"))
	assert.Nil(t, err)
	assert.Equal(t, is1, true)

	is2, err := rdb.SIsMember(util.GetTestKey(3), []byte("test-3"))
	assert.Nil(t, err)
	assert.Equal(t, is2, false)

	del1, err := rdb.SRem(util.GetTestKey(2), []byte("test-2"))
	assert.Nil(t, err)
	assert.Equal(t, del1, true)

	is3, err := rdb.SIsMember(util.GetTestKey(2), []byte("test-2"))
	assert.Nil(t, err)
	assert.Equal(t, is3, false)
}

func TestRedisData_List(t *testing.T) {
	opts := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-set")
	opts.DataDir = dir
	rdb, err := NewRedisDB(opts)
	assert.Nil(t, err)

	var val1 []byte = []byte("test2")
	s1, err := rdb.LPush(util.GetTestKey(2), val1)
	assert.Nil(t, err)
	assert.Equal(t, s1, uint32(1))

	var val2 []byte = []byte("test3")
	s2, err := rdb.RPush(util.GetTestKey(2), val2)
	assert.Nil(t, err)
	assert.Equal(t, s2, uint32(2))

	el1, err := rdb.LPop(util.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, el1, val1)

	var val3 []byte = []byte("test3")
	s3, err := rdb.LPush(util.GetTestKey(2), val3)
	assert.Nil(t, err)
	assert.Equal(t, s3, uint32(2))
}
