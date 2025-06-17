package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // Normal log record
	LogRecordDeleted
)

// crc32(4) type(1) KeySize(5) ValueSize(5) = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 4 + 1

// LogRecord 表示一次数据记录，采用追加写，类似日志
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// LogRecordPos 用于内存中的索引，可以用来索引到磁盘中具体的文件以及所在的文件的偏移位置
type LogRecordPos struct {
	Fid    uint32 //表示数据存储在哪个文件中
	Offset int64  //表示数据存储在文件中的偏移量
}

// 将LogRecord序列化成字节数组
// |<-------------Header(Max-20Bytes)------------------|
// +------------+------------+------------+------------+------------+------------+
// |    crc     |    type    |  key size  | value size |   key data | value data |
// +------------+------------+------------+------------+------------+------------+
//
//	4Bytes        1Bytes     Max-5Bytes  Max-5Bytes
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//由于key和value都是byte array，我们只用编码header即可
	header := make([]byte, maxLogRecordHeaderSize)

	var index int = 4
	header[index] = logRecord.Type
	index++

	//放置key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	//放置value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	//到此，index就是header的长度
	var totalSize = index + len(logRecord.Key) + len(logRecord.Value)

	encryptBytes := make([]byte, totalSize)

	//将header放到最后的字节数组中
	//copy(dst, src)
	copy(encryptBytes[:index], header[:index])

	//将key和value分别放进去
	copy(encryptBytes[index:], logRecord.Key)
	copy(encryptBytes[index+len(logRecord.Key):], logRecord.Value)

	//对整个数据进行crc校验，然后放到前四个字节中
	crc := crc32.ChecksumIEEE(encryptBytes[4:])
	binary.LittleEndian.PutUint32(encryptBytes[:4], crc)

	//fmt.Printf("header size:%d, crc:%d, keySize:%d, valueSize:%d, key:%s, value:%s\n", index, crc, len(logRecord.Key), len(logRecord.Value), string(logRecord.Key), string(logRecord.Value))
	return encryptBytes, int64(totalSize)
}

// 解码LogRecord的头部
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	//首先取出前四个字节的crc校验码
	crcVal := binary.LittleEndian.Uint32(buf[:4])

	//1个字节的type
	tp := buf[4]

	var index uint32 = 5

	//读取可变长度key size
	keyLen, kSize := binary.Varint(buf[index:])
	//读取可变长度value size
	valueLen, vSize := binary.Varint(buf[index+uint32(kSize):])

	var headerSize = int(index) + kSize + vSize
	return &LogRecordHeader{
		crc:        crcVal,
		recordType: tp,
		keySize:    uint32(keyLen),
		valueSize:  uint32(valueLen),
	}, int64(headerSize)
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc
}
