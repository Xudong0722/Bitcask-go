package config

import (
	"Bitcask-go/util"
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

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)
