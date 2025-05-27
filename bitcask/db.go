package bitcask

import (
	"Bitcask-go/config"
	"Bitcask-go/data"
	"Bitcask-go/index"
	"Bitcask-go/util"
	"sync"
)

type DB struct {
	mutex         *sync.RWMutex
	activeFile    *data.DataFile            //当前活跃的文件
	olderFiles    map[uint32]*data.DataFile //旧文件，只能用于读
	configuration config.Configuration
	index         index.Indexer
}

// 写入数据到DB中， key不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(log_record)

	if err != nil {
		return err
	}

	//写入到磁盘中之后，更新内存中的索引
	if ok := db.index.Put(key, pos); !ok {
		return util.ErrIndexUpdateFailed
	}

	return nil
}

// 从DB中获取数据，key不能为空
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if len(key) == 0 {
		return nil, util.ErrKeyIsEmpty
	}

	//先从内存索引中获取对应的LogRecordPos
	pos := db.index.Get(key)
	if pos == nil {
		return nil, util.ErrKeyNotFound
	}

	//根据LogRecordPos从数据文件中读取对应的LogRecord
	var data_file *data.DataFile
	if db.activeFile.Fid == pos.Fid {
		data_file = db.activeFile
	} else {
		data_file = db.olderFiles[pos.Fid]
	}

	if data_file == nil {
		return nil, util.ErrDataFileNotFound
	}

	log_record, err := data_file.ReadAt(pos.Offset)
	if err != nil {
		return nil, err
	}

	//如果读取的记录是删除的，则返回key不存在
	if log_record.Type == data.LogRecordDeleted {
		return nil, util.ErrKeyNotFound
	}

	return log_record.Value, nil
}

// 追加日志记录到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	//判断当前是否有活跃文件，如果没有，则创建一个
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, len := data.EncodeLogRecord(logRecord)

	//如果写入的文件不能够容纳新的记录，则将当前活跃文件关闭，并创建一个新的活跃文件
	if db.activeFile.WriteOffset+len > db.configuration.DataFileMaxSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.Fid] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.configuration.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.Fid,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前活跃的数据文件
func (db *DB) setActiveDataFile() error {
	var initialFid uint32 = 0

	if db.activeFile != nil {
		initialFid = db.activeFile.Fid + 1
	}

	dataFile, err := data.OpenDataFile(db.configuration.DataDir, initialFid)

	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}
