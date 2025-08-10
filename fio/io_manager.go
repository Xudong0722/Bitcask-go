package fio

const FilePermission = 0644

type FileIOType = byte

const (
	//标准文件IO
	StandardFIO FileIOType = iota

	//MemoryMap 内存文件映射
	MemoryMap
)

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

// 初始化一个IOManager， 目前只有FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
