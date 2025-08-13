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
