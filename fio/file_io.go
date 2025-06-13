package fio

import (
	"os"
)

// FileIO 标准系统文件IO, 可以组合到我们的DataFile对象中
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 创建一个新的FileIO实例
func NewFileIOManager(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		FilePermission,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

//从文件的指定位置读取数据
func (fio *FileIO) Read(bs []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(bs, offset)
}

//写文件
func (fio *FileIO) Write(bs []byte) (int, error) {
	return fio.fd.Write(bs)
}

//将文件同步到磁盘
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

//关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

//返回文件大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
