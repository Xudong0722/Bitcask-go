package test

import (
	"Bitcask_go/data"
	"Bitcask_go/fio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 111)+data.DataFileSuffix)
	err := os.Remove(fileName)
	assert.Nil(t, err)
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
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
	fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 111)+data.DataFileSuffix)
	err := os.Remove(fileName)
	assert.Nil(t, err)
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("Test Close"))
	assert.Nil(t, err)

	err = dataFile1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 111)+data.DataFileSuffix)
	err := os.Remove(fileName)
	assert.Nil(t, err)
	dataFile1, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("Test Sync"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 111)+data.DataFileSuffix)
	err := os.Remove(fileName)
	assert.Nil(t, err)

	dataFile, err := data.OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条LogRecord
	rec1 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcase kv go"),
		Type:  data.LogRecordNormal,
	}

	res1, size1 := data.EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(size1)

	//再加一条log，测试带偏移量的读取
	rec2 := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}

	res2, size2 := data.EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)
	t.Log(size2)
}
