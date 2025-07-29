package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/data"
	"Bitcask_go/util"
	"sync"
	"sync/atomic"
)

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
		pendingWrites: map[string]*data.LogRecord,
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

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1) //事务序列号，全局递增

}
