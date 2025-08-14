package redis

import (
	bitcask "Bitcask_go"
	"Bitcask_go/config"
	"Bitcask_go/util"
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

//==================== Hash ====================

// myhash    a       100
//
//	key     field   value
func (rdb *RedisDB) HSet(key, field, value []byte) (bool, error) {
	meta, err := rdb.findMetadata(key, RHash)

	if err != nil {
		return false, err
	}

	//构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	encHk := hk.encode()

	//查找当前的key是否存在
	var exist = true
	if _, err = rdb.db.Get(encHk); err == util.ErrKeyNotFound {
		exist = false
	}

	wb := rdb.db.NewWriteBatch(config.DefaultWriteBatchOptions)

	// 如果不存在，说明这个field不存在，则size+1， 更新元数据
	// 如果存在，说明这个field存在，元数据不变
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(encHk, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rdb *RedisDB) HGet(key, field []byte) ([]byte, error) {
	meta, err := rdb.findMetadata(key, RHash)

	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	//构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	return rdb.db.Get(hk.encode())
}

func (rdb *RedisDB) HDel(key, field []byte) (bool, error) {
	meta, err := rdb.findMetadata(key, RHash)

	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	//构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encHk := hk.encode()

	//查看是否存在
	var exist = true
	if _, err = rdb.db.Get(encHk); err == util.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rdb.db.NewWriteBatch(config.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encHk)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// 查找元数据，如果不存在返回一个初始化的metadata
func (rdb *RedisDB) findMetadata(key []byte, dataType redisDataStructureType) (*metadata, error) {
	encMeta, err := rdb.db.Get(key)
	if err != nil && err != util.ErrKeyNotFound {
		return nil, err
	}

	var exist = true
	var meta *metadata
	if err == util.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(encMeta)

		//校验type和过期时间
		if meta.dataType != byte(dataType) {
			return nil, ErrWrongTypeOperation
		}

		if meta.expire > 0 && time.Now().UnixNano() >= meta.expire {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: byte(dataType),
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == RList {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}
