package index

import (
	"Bitcask_go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// 自适应基数树索引
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中添加key对应的位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldItem, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	return oldItem.(*data.LogRecordPos)
}

// Get 根据key获取索引中对应的位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	pos, ok := art.tree.Search(key)
	art.lock.RUnlock()
	if !ok {
		return nil
	}
	return pos.(*data.LogRecordPos)
}

// Delete 删除索引中key对应的位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldItem, ok := art.tree.Delete(key)
	art.lock.Unlock()
	return oldItem.(*data.LogRecordPos), ok
}

// Size 获取索引中元素的数量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Iterator 获取索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type artIterator struct {
	currIndex int     //当前索引的下标
	reverse   bool    //是否反向迭代
	values    []*Item //存储迭代器位置对应的 Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	if tree.Size() == 0 {
		return nil
	}

	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())

	// 遍历 art 并将所有元素存储到 values 中
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}

		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (art *artIterator) Rewind() {
	art.currIndex = 0
}

func (art *artIterator) Seek(key []byte) {
	if art.reverse {
		// 反向查找第一个小于等于 key 的元素
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) <= 0
		})
	} else {
		// 正向查找第一个大于等于 key 的元素
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) >= 0
		})
	}
}

func (art *artIterator) Next() {
	art.currIndex += 1
}

func (art *artIterator) Valid() bool {
	return art.currIndex >= 0 && art.currIndex < len(art.values)
}

func (art *artIterator) Key() []byte {
	if art.Valid() {
		return art.values[art.currIndex].key
	}
	return nil
}

func (art *artIterator) Value() *data.LogRecordPos {
	if art.Valid() {
		return art.values[art.currIndex].pos
	}
	return nil
}
func (art *artIterator) Close() {
	art.values = nil // 清理迭代器中的数据
	art.currIndex = 0
}
