package index

import (
	"Bitcask_go/data"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
	"go.etcd.io/bbolt"
)

type BPlusTree struct {
	tree bbolt.DB  //already concurrent access
}

func NewBPTree() *BPlusTree {
	return &BPlusTree{
		tree: bbolt.Open()
	}
}

// Put 向索引中添加key对应的位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

// Get 根据key获取索引中对应的位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	pos, ok := art.tree.Search(key)
	art.lock.RUnlock()
	if !ok {
		return nil
	}
	return pos.(*data.LogRecordPos)
}

// Delete 删除索引中key对应的位置信息
func (bpt *BPlusTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, ok := art.tree.Delete(key)
	art.lock.Unlock()
	return ok
}

// Size 获取索引中元素的数量
func (bpt *BPlusTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Iterator 获取索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}
