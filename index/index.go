package index

import (
	"Bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

//Indexer 索引 接口
type Indexer interface {
	// Put 向索引中添加key对应的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据key获取索引中对应的位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引中key对应的位置信息
	Delete(key []byte) bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1

	//自适应基树索引
	ART
)

func NewIndexer(type IndexerType) Indexer {
	switch(type) {
	case BTree :
		return NewBTree()
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
