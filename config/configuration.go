package config

type Configuration struct {
	DataDir         string //数据文件存放的目录
	DataFileMaxSize int64  //数据文件的最大大小，单位为字节
	SyncWrites      bool   //是否同步写入数据到磁盘
}
