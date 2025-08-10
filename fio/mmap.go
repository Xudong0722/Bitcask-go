package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, FilePermission)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// Read 从文件的指定位置读取数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// Write 向文件中写入数据
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync 将内存中的数据同步到磁盘
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
