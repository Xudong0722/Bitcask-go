package data

import "Bitcask-go/fio"

const DataFileSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	Fid         uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

//打开指定路径的数据文件
func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	return nil, nil
}

//同步到磁盘中
func (df *DataFile) Sync() error {
	return nil
}

//写入到文件中，会先写入缓冲区
func (df *DataFile) Write(buf []byte) error {
	return nil
}

//从指定偏移读取数据， TODO：指定读取多长的数据
func (df *DataFile) ReadAt(offset int64) (*LogRecord, error) {
	return nil, nil
}
