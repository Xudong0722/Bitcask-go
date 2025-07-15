package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/index"
)

// Iterator 索引迭代器接口
type Iterator struct {
	indexIter index.Iterator         // 索引迭代器
	db        *DB                    // 数据库实例
	options   config.IteratorOptions // 迭代器配置
}

func (db *DB) NewIterator(ops config.IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(ops.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   ops,
	}
}

// Rewind 重置迭代器
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.SkipToNext() // 如果有前缀，则跳过不匹配的元素
}

// Seek 根据key查找第一个大于(或小于)等于key的元素
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.SkipToNext() // 如果有前缀，则跳过不匹配的元素
}

// Next 移动到下一个元素
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.SkipToNext() // 如果有前缀，则跳过不匹配的元素
}

// Valid 是否有效
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 获取当前元素的key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 获取当前元素的位置信息
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mutex.RLock()
	defer it.db.mutex.RUnlock()

	val, err := it.db.GetValueByPosition(logRecordPos)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Close 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) SkipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return // 没有前缀，直接返回
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if len(key) >= prefixLen && string(key[:prefixLen]) == string(it.options.Prefix) {
			break
		}
	}
}
