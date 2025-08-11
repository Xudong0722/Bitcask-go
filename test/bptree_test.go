package test

import (
	"Bitcask_go/data"
	"Bitcask_go/index"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := index.NewBPTree(path, false)

	res1 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	assert.Nil(t, res1)

	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.NotNil(t, res2)
	assert.Equal(t, res2.Fid, uint32(123))
	assert.Equal(t, res2.Offset, int64(9999))

	res3 := tree.Put([]byte("sfdsfdsfdsfs"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res3)
}

func TestNewBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := index.NewBPTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos = tree.Get([]byte("abc"))
	assert.NotNil(t, pos)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 43543, Offset: 432543})
	pos = tree.Get([]byte("abc"))
	assert.NotNil(t, pos)
}

func TestNewBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := index.NewBPTree(path, false)

	res1, ok := tree.Delete([]byte("not exist"))
	assert.Equal(t, ok, false)
	assert.Nil(t, res1)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos := tree.Get([]byte("abc"))
	assert.NotNil(t, pos)

	tree.Delete([]byte("abc"))
	pos = tree.Get([]byte("abc"))
	assert.Nil(t, pos)
}

func TestNewBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := index.NewBPTree(path, false)

	res1, ok := tree.Delete([]byte("not exist"))
	assert.Equal(t, ok, false)
	assert.Nil(t, res1)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos := tree.Get([]byte("abc"))
	assert.NotNil(t, pos)

	assert.Equal(t, 1, tree.Size())

	tree.Delete([]byte("abc"))
	pos = tree.Get([]byte("abc"))
	assert.Nil(t, pos)

	assert.Equal(t, 0, tree.Size())
}

func TestBplusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := index.NewBPTree(path, false)

	tree.Put([]byte("123"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("321"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("471"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("431"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("312"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("132"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

	iter1 := tree.Iterator(true)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log(string(iter1.Key()))
	}
}
