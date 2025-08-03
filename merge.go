package Bitcask_go

import (
	"Bitcask_go/data"
	"Bitcask_go/util"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	// 如果数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}

	db.mutex.Lock()
	// merge 正在进行中，返回报错信息
	if db.isMerging {
		db.mutex.Unlock()
		return util.ErrMergeisInProgress
	}
	defer func() {
		db.isMerging = false
	}()

	db.isMerging = true

	//将当前活跃文件转换成旧文件，保存到数组中，然后开展merge
	if err := db.activeFile.Sync(); err != nil {
		db.mutex.Unlock()
		return err
	}

	db.olderFiles[db.activeFile.Fid] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mutex.Unlock()
		return err
	}
	//记录最近的一个没有参加merge的文件id
	nonMergeFileId := db.activeFile.Fid

	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	//之后就不需要加锁了，我们只merge旧文件
	db.mutex.Unlock()

	// 将所有文件按从小到大顺序排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].Fid < mergeFiles[j].Fid
	})

	mergePath := db.getMergePath()
	// 如果当前目录存在， 将其删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建一个merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeConfig := db.configuration
	mergeConfig.DataDir = mergePath
	mergeConfig.SyncWrites = false

	mergeDB, err := Open(mergeConfig)
	if err != nil {
		return err
	}

	//打开一个Hint文件存储索引位置信息
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历处理每个datafile
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			realKey, _ := parseLogRecordKeyWithSeq(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//和内存中的索引位置进行比较，如果有效就重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.Fid && logRecordPos.Offset == offset {
				//这已经是一条有效数据了，不需要事务序列号了
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				//这是一个新的DB，只有merge操作会操作此DB，所以可以用无锁版本
				pos, err := mergeDB.appendLogRecord(logRecord)
				//因为数据被重新写入了，所以位置信息变了，后续索引需要更新
				if err != nil {
					return err
				}
				//将当前位置索引写入到Hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	//Sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge完成的文件
	mergeFinFile, err := data.OpenMergeFinFile(mergePath)
	if err != nil {
		return err
	}
	//merge完成文件中会有一条最近没有参加merge的文件id，小于此id的文件都参与了
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

// eg. current db dir: /tmp/bitcask-go
// Invoke this func will acquire /tmp/bitcask-go-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.configuration.DataDir)) // /tmp
	base := path.Base(db.configuration.DataDir)           // bitcask-go
	return path.Join(dir, base+mergeDirName)
}

// 加载merge数据目录
func (db *DB) loadMergeFiles() error {

}
