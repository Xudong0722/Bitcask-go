package test

import (
	"Bitcask-go/data"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := data.OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := data.OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := data.OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("Test write"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("append - 1"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("append - 2"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("Test Close"))
	assert.Nil(t, err)

	err = dataFile1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("Test Sync"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}
