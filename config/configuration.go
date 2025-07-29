package config

import (
	"Bitcask_go/util"
	"os"
)

type Configuration struct {
	DataDir         string      //数据文件存放的目录
	DataFileMaxSize int64       //数据文件的最大大小，单位为字节
	SyncWrites      bool        //是否同步写入数据到磁盘
	IndexerType     IndexerType //索引的类型
}

func CheckCfg(cfg Configuration) error {
	if cfg.DataDir == "" {
		return util.ErrDataDirEmpty
	}
	if cfg.DataFileMaxSize <= 0 {
		return util.ErrDataFileMaxSizeInvalid
	}
	return nil
}

// IteratorOptions 索引迭代器配置
type IteratorOptions struct {
	Prefix  []byte //遍历前缀和为指定值的Key，默认为空
	Reverse bool   //是否反向迭代
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Configuration{
	DataDir:         os.TempDir(),
	DataFileMaxSize: 256 * 1024 * 1024, //256MB
	SyncWrites:      false,
	IndexerType:     Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	MaxBatchNum uint //单频次可写入的最大数量
	SyncWrite   bool //是否同步到磁盘
}
