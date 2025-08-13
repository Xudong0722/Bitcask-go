package redis

import (
	bitcask "Bitcask_go"
	"Bitcask_go/config"
	"encoding/binary"
	"errors"
	"time"
)

type redisDataStructureType byte

const (
	RUnknown redisDataStructureType = iota
	RString
	RHash
	RSet
	RList
	RZSet
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrKeyIsExpired       = errors.New("THE Current key is expired")
)

type RedisDB struct {
	db *bitcask.DB
}

func NewRedisDB(cfg config.Configuration) (*RedisDB, error) {
	db, err := bitcask.Open(cfg)
	if err != nil {
		return nil, err
	}
	return &RedisDB{db: db}, nil
}

//==================== String ====================

func (rdb *RedisDB) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// value : type(1 byte) + expire(n byte) + payload(n byte)
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = byte(RString)
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return rdb.db.Put(key, encValue)
}

func (rdb *RedisDB) Get(key []byte) ([]byte, error) {
	encValue, err := rdb.db.Get(key)
	if err != nil {
		return nil, err
	}

	//如果类型不匹配，返回相应错误
	if encValue[0] != byte(RString) {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	//如果已经过期，就返回空
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, ErrKeyIsExpired
	}

	return encValue[index:], nil
}
