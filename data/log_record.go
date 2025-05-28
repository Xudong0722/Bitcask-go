package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // Normal log record
	LogRecordDeleted
)

// LogRecord 表示一次数据记录，采用追加写，类似日志
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 用于内存中的索引，可以用来索引到磁盘中具体的文件以及所在的文件的偏移位置
type LogRecordPos struct {
	Fid    uint32 //表示数据存储在哪个文件中
	Offset int64  //表示数据存储在文件中的偏移量
}

// 将LogRecord序列化成字节数组
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}
