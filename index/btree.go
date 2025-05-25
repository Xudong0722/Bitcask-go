package index

import (
	"Bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

//BTree 索引，ref google btree

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex //BTree的读写锁
}

func NewBTree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btree_item := bt.tree.Get(it)
	if btree_item == nil {
		return nil
	}
	return btree_item.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	btree_item := bt.tree.Delete(it)
	bt.lock.Unlock()
	return btree_item != nil
}
