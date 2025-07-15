package test

import (
	"Bitcask_go/data"
	"Bitcask_go/index"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := index.NewBTree(32)

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([](byte)("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := index.NewBTree(32)

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([](byte)("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([](byte)("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([](byte)("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := index.NewBTree(32)

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([](byte)("test"), &data.LogRecordPos{Fid: 2, Offset: 200})
	assert.True(t, res3)

	res4 := bt.Delete([](byte)("test"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := index.NewBTree(32)
	//1.BTree为空的情况
	it1 := bt1.Iterator(false)
	assert.Equal(t, it1.Valid(), false)

	//2.BTree有数据的情况
	bt1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 1})
	it2 := bt1.Iterator(false)
	assert.Equal(t, it2.Valid(), true)
	assert.Equal(t, it2.Key(), []byte("c"))
	assert.NotNil(t, it2.Value())
	it2.Next()
	assert.Equal(t, it2.Valid(), false)

	//3.BTree有多条数据的情况-正向遍历
	bt1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 2})
	bt1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	it3 := bt1.Iterator(false)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		//t.Log("Key:", string(it3.Key()))
		assert.NotNil(t, it3.Value())
	}

	//4.BTree有多条数据的情况-反向遍历
	it4 := bt1.Iterator(true)
	for it4.Rewind(); it4.Valid(); it4.Next() {
		//t.Log("Key:", string(it4.Key()))
		assert.NotNil(t, it4.Value())
	}

	//4. BTree有多条数据的情况-Seek
	it5 := bt1.Iterator(false)
	it5.Seek([]byte("b"))
	assert.Equal(t, it5.Valid(), true)
	assert.Equal(t, it5.Key(), []byte("b"))
	assert.NotNil(t, it5.Value())

	for it5.Seek([]byte("b")); it5.Valid(); it5.Next() {
		assert.NotNil(t, it5.Value())
	}

	//5.BTree有多条数据的情况-反向遍历-Seek
	it6 := bt1.Iterator(true)
	it6.Seek([]byte("z"))
	t.Log("Seek Key:", string(it6.Key()))
	assert.Equal(t, it6.Valid(), true)
	assert.Equal(t, it6.Key(), []byte("c"))
	assert.NotNil(t, it6.Value())
}
