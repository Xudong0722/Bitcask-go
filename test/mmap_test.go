package test

import (
	"Bitcask_go/fio"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestNewFileIOManager(t *testing.T) {
// 	path := filepath.Join("/tmp", "a.data")
// 	fio, err := fio.NewFileIOManager(path)
// 	defer destoryFile(path) // Clean up after test

// 	assert.Nil(t, err)
// 	assert.NotNil(t, fio)
// }

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destoryFile(path)

	mmapIO, err := fio.NewMMapIOManager(path)
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, n1, 0)
	assert.NotNil(t, err)

	fileIO, err := fio.NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("123"))
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("456"))
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("789"))
	assert.Nil(t, err)

	mmapIO2, err := fio.NewMMapIOManager(path)
	assert.Nil(t, err)

	t.Log(mmapIO2.Size())
}

func TestMMap_Close(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := fio.NewFileIOManager(path)
	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
