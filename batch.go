package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/data"
	"Bitcask_go/util"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var finishKey = []byte("fin-key")

const nonTransactionSeqNo uint64 = 0

type WriteBatch struct {
	options       config.WriteBatchOptions   //配置项
	mu            *sync.Mutex                //互斥锁，多个线程如果使用一个WriteBatch去写，保证线程安全
	pendingWrites map[string]*data.LogRecord //存放待写的数据
	db            *DB                        //所属DB实例
}

func (db *DB) NewWriteBatch(opts config.WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		pendingWrites: make(map[string]*data.LogRecord),
		db:            db,
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	//将此数据暂存到内存中
	logRecord := data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = &logRecord
	return nil
}

// Delete 删除某个数据, 如果索引中不存在此数据就直接返回
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = &logRecord
	return nil
}

// Commit 事务提交，将pendingWrites中的数据全部写到数据文件中，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > int(wb.options.MaxBatchNum) {
		return util.ErrExceedMaxBatchNum
	}

	//保证事务处理的串行化
	wb.db.mutex.Lock()
	defer wb.db.mutex.Unlock()

	//获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 将数据写入到data file中
	positions := make(map[string]*data.LogRecordPos)

	for _, record := range wb.pendingWrites {
		//写入到datafile中，注意这里使用无锁版本，前面已经加锁了
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})

		if err != nil {
			return err
		}

		positions[string(record.Key)] = logRecordPos
	}

	//写一条标记事务完成的record
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(finishKey, seqNo),
		Type: data.LogRecordFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置决定当前是否要同步到磁盘
	if wb.options.SyncWrite && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, record := range wb.pendingWrites {
		logRecordPos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, logRecordPos)
		} else if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 将key和序列号
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

func parseLogRecordKeyWithSeq(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
