package data

//LogRecordPos represents the position of data record in the disk
type LogRecordPos struct {
    file_id uint32   //表示数据存储在哪个文件中
    offset  uint32   //表示数据存储在文件中的偏移量
}