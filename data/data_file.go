package data

import "Bitcask-go/fio"

// DataFile 数据文件
type DataFile struct {
	Fid         uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadAt(offset int64) (*LogRecord, error) {
	return nil, nil
}
