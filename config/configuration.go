package config

import (
	"Bitcask-go/util"
)
type Configuration struct {
	DataDir         string //数据文件存放的目录
	DataFileMaxSize int64  //数据文件的最大大小，单位为字节
	SyncWrites      bool   //是否同步写入数据到磁盘
	indexerType     IndexerType  //索引的类型
}

func checkCfg(cfg Configuration) error {
	if cfg.DataDir == "" {
		return ErrDataDirEmpty
	}
	if cfg.DataFileMaxSize <= 0 {
		return ErrDataFileMaxSizeInvalid
	}
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)