package fio

const FilePermission = 0644

// IOManager
type IOManager interface {
	//Read 从文件的指定位置读取数据
	Read([]byte, int64) (int, error)

	//Write 向文件中写入数据
	Write([]byte) (int, error)

	//Sync 将内存中的数据同步到磁盘
	Sync() error

	//Close 关闭文件
	Close() error
}
