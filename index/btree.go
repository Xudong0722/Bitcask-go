package index

import (
	"Bitcask_go/data"
	"bytes"
	"sort"
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

type btreeIterator struct {
	currIndex int     //当前索引的下标
	reverse   bool    //是否反向迭代
	values    []*Item //存储迭代器位置对应的 Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 遍历 BTree 并将所有元素存储到 values 中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values[:idx], // 截取有效部分
	}
}

func (it *btreeIterator) Rewind() {
	it.currIndex = 0
}

func (it *btreeIterator) Seek(key []byte) {
	if it.reverse {
		// 反向查找第一个小于等于 key 的元素
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) <= 0
		})
	} else {
		// 正向查找第一个大于等于 key 的元素
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) >= 0
		})
	}
}
