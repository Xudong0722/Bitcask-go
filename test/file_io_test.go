package test

import (
	"Bitcask_go/fio"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path) // Clean up after test

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world"))
	assert.Equal(t, 11, n)
	assert.Nil(t, err)
	t.Log(n, nil)

	n, err = fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
	t.Log(n, nil)

	n, err = fio.Write([]byte("hello world again"))
	assert.Equal(t, 17, n)
	assert.Nil(t, err)
	t.Log(n, nil)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("hello world"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("hello world again"))
	assert.Nil(t, err)

	bs := make([]byte, 11)
	n, err := fio.Read(bs, 0)
	assert.Equal(t, 11, n)
	assert.Nil(t, err)
	assert.Equal(t, "hello world", string(bs))
	t.Log(n, string(bs))

	bs = make([]byte, 17)
	n, err = fio.Read(bs, 11)
	assert.Equal(t, 17, n)
	assert.Nil(t, err)
	assert.Equal(t, "hello world again", string(bs))
	t.Log(n, string(bs))
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
