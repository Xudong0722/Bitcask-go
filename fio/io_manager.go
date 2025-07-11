package fio

const FilePermission = 0644

// IOManager， 文件IO的接口
type IOManager interface {
	//Read 从文件的指定位置读取数据
	Read([]byte, int64) (int, error)

	//Write 向文件中写入数据
	Write([]byte) (int, error)

	//Sync 将内存中的数据同步到磁盘
	Sync() error

	//Close 关闭文件
	Close() error

	//Size 获取文件大小
	Size() (int64, error)
}

//初始化一个IOManager， 目前只有FileIO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
