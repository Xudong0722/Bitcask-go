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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()

	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btree_item := bt.tree.Get(it)
	if btree_item == nil {
		return nil
	}
	return btree_item.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	btree_item := bt.tree.Delete(it)
	bt.lock.Unlock()

	return btree_item.(*Item).pos, btree_item != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
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

func (it *btreeIterator) Next() {
	it.currIndex += 1
}

func (it *btreeIterator) Valid() bool {
	return it.currIndex >= 0 && it.currIndex < len(it.values)
}

func (it *btreeIterator) Key() []byte {
	if it.Valid() {
		return it.values[it.currIndex].key
	}
	return nil
}

func (it *btreeIterator) Value() *data.LogRecordPos {
	if it.Valid() {
		return it.values[it.currIndex].pos
	}
	return nil
}
func (it *btreeIterator) Close() {
	it.values = nil // 清理迭代器中的数据
	it.currIndex = 0
}
