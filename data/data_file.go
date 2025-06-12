package data

import (
	"Bitcask-go/fio"
	"Bitcask-go/util"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	Fid         uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

// 打开指定路径的数据文件
func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+DataFileSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		Fid:         fid,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// 同步到磁盘中
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// 写入到文件中，会先写入缓冲区
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// 从数据文件中根据指定偏移读取LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//获取文件大小
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	//如果文件剩下的大小 小于 maxLogRecordHeaderSize，我们就只读剩下的即可
	if headerBytes > fileSize-offset {
		headerBytes = fileSize - offset
	}

	//读取header信息
	headerBuf, err := df.readBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	//读到文件末尾
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	logRecordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}

	if keySize > 0 && valueSize > 0 {
		kvData, err := df.readBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvData[:keySize]
		logRecord.Value = kvData[keySize:]
	}

	//检验数据有效性, 注意headerBuf是按最大长度取得，所以我们这里要取有效部分
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, util.ErrInvalidCRC
	}

	return logRecord, logRecordSize, nil
}

func (df *DataFile) readBytes(n, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
