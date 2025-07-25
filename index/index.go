package index

import (
	"Bitcask_go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer 索引 接口
type Indexer interface {
	// Put 向索引中添加key对应的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据key获取索引中对应的位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引中key对应的位置信息
	Delete(key []byte) bool

	// Size 获取索引中元素的数量
	Size() int

	// Iterator 获取索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1

	//自适应基树索引
	ART
)

func NewIndexer(tp IndexerType) Indexer {
	switch tp {
	case Btree:
		return NewBTree(32)
	case ART:
		return nil
	default:
		panic("unsupport indexer type.")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

// Iterator 索引迭代器接口
type Iterator interface {
	//Rewind 重置迭代器
	Rewind()

	//Seek 根据key查找第一个大于(或小于)等于key的元素
	Seek(key []byte)

	//Next 移动到下一个元素
	Next()

	//Valid 是否有效
	Valid() bool

	//Key 获取当前元素的key
	Key() []byte

	//Value 获取当前元素的位置信息
	Value() *data.LogRecordPos

	//Close 关闭迭代器
	Close()
}
