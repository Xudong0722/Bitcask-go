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
		_ = os.Remove(path)
	}()

	tree := index.NewBPTree(path)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("123"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("sfdsfdsfdsfs"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestNewBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()

	tree := index.NewBPTree(path)

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
		_ = os.Remove(path)
	}()

	tree := index.NewBPTree(path)

	res1 := tree.Delete([]byte("not exist"))
	assert.Equal(t, res1, false)

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
		_ = os.Remove(path)
	}()

	tree := index.NewBPTree(path)

	res1 := tree.Delete([]byte("not exist"))
	assert.Equal(t, res1, false)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos := tree.Get([]byte("abc"))
	assert.NotNil(t, pos)

	assert.Equal(t, 1, tree.Size())

	tree.Delete([]byte("abc"))
	pos = tree.Get([]byte("abc"))
	assert.Nil(t, pos)

	assert.Equal(t, 0, tree.Size())
}
