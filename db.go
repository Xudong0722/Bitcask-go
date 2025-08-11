package Bitcask_go

import (
	"Bitcask_go/config"
	"Bitcask_go/data"
	"Bitcask_go/fio"
	"Bitcask_go/index"
	"Bitcask_go/util"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

type DB struct {
	mutex           *sync.RWMutex             //读写锁，保证线程安全
	activeFile      *data.DataFile            //当前活跃的文件
	olderFiles      map[uint32]*data.DataFile //旧文件，只能用于读
	configuration   config.Configuration      //用户配置项
	index           index.Indexer             //内存索引, [key, LogRecordPos]
	fds             []int                     //已排序的文件id，只用于加载索引
	seqNo           uint64                    //事务序列号，全局递增
	isMerging       bool                      //数据库是否正在merge
	seqNoFileExists bool                      //存储数据库事务序列号的文件是否存在
	isInitial       bool                      //是否是第一次使用此数据目录
	fileLock        *flock.Flock              //文件锁，保证当前数据
	bytesWrite      uint                      //累计写了多少个字节
	reclaimSize     int64                     //表示DB中有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  //key的总数量
	DataFileNum     uint  //数据文件的总数量
	ReclaimableSize int64 //merge后可回收的数据大小，单位byte
	DiskSize        int64 //数据引擎占据磁盘大小
}

// 通过配置项构造一个DB
func Open(cfg config.Configuration) (*DB, error) {
	// 先检查配置是否有效
	if err := config.CheckCfg(cfg); err != nil {
		return nil, err
	}

	var isInitial bool
	//如果用户配置的文件目录不存在，则创建这个目录
	if _, err := os.Stat(cfg.DataDir); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(cfg.DataDir, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}

	if !hold {
		return nil, util.ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	//如果目录存在但里面为空，也视为第一次使用
	if len(entries) == 0 {
		isInitial = true
	}

	//初始化 DB
	db := &DB{
		mutex:         new(sync.RWMutex),
		olderFiles:    make(map[uint32]*data.DataFile),
		configuration: cfg,
		index:         index.NewIndexer(cfg.IndexerType, cfg.DataDir, cfg.SyncWrites),
		seqNo:         0,
		isInitial:     isInitial,
		fileLock:      fileLock,
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//从磁盘中加载所有的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//如果是B+树索引，不需要从数据文件加载索引了
	if cfg.IndexerType != config.BPTree {
		//从hint文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		//从数据文件构建索引
		if err := db.LoadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		//将文件IO类型重置为标准IO
		if db.configuration.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}
	} else {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		//更新当前活跃数据文件的WriteOffset
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}

			db.activeFile.WriteOffset = size
		}
	}

	return db, nil
}

// Close 关闭DB，释放资源
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}

		//BPtree索引类型底层也是一个文件，需要关闭
		if err := db.index.Close(); err != nil {
			panic("failed to close index")
		}
	}()

	if db.activeFile == nil {
		return nil
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	//如果是B+树索引，需要将seqNo写入到文件中，后续加载数据文件的话可以从此文件中获取
	seqNoFile, err := data.OpenSeqNoFile(db.configuration.DataDir)
	if err != nil {
		return err
	}

	//构造包含seqNo数据的LogRecord
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}

	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//关闭活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	//关闭所有旧文件
	for _, of := range db.olderFiles {
		if of != nil {
			if err := of.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Sync 持久化数据
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	//同步活跃文件到磁盘
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	return nil
}

// 写入数据到DB中， key不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	//构造一个LogRecord，准备写入到磁盘的数据文件中
	log_record := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// db.mutex.Lock()   //写锁不可重入
	// defer db.mutex.Unlock()

	//追加写入到磁盘的活跃文件中
	pos, err := db.appendLogRecordWithLock(log_record)

	if err != nil {
		return err
	}

	//写入到磁盘中之后，更新内存中的索引(TODO, 这里会不会写磁盘成功，更新内存失败，造成数据不一致)
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	return db.GetValueByPosition(pos)
}

// 根据LogRecordPos获取对应的Value
func (db *DB) GetValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	if pos == nil {
		return nil, util.ErrKeyNotFound
	}

	//fmt.Printf("[Get], fid:%d, offset:%d\n", pos.Fid, pos.Offset)

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

	log_record, _, err := data_file.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	//如果读取的记录是删除的，则返回key不存在
	if log_record.Type == data.LogRecordDeleted {
		return nil, util.ErrKeyNotFound
	}

	return log_record.Value, nil
}

// ListKyes 获取所有的key
func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 遍历所有的key，执行fn函数. 如果fn返回false，则停止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value, err := db.GetValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(key, value) {
			break
		}
	}
	return nil
}

// 删除某条数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	//如果key不存在，直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//先构造一条删除记录，追加写入DB
	logRecord := data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted}

	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return util.ErrDataDeleteFailed
	}
	//delete这条记录本身也是可以删除的
	db.reclaimSize += int64(pos.Size)

	//然后删除内存索引中的记录
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return util.ErrDataDeleteFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// 追加日志记录到活跃文件中 - 有锁版本
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加日志记录到活跃文件中 - 无锁版本
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前是否有活跃文件，如果没有，则创建一个
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//对数据进行编码，返回byte[]
	encRecord, len := data.EncodeLogRecord(logRecord)

	//.Print(encRecord) //Debug info

	//如果写入的文件已经不能够容纳新的记录，则将当前活跃文件关闭，并创建一个新的活跃文件
	if db.activeFile.WriteOffset+len > db.configuration.DataFileMaxSize {
		//将当前活跃文件写入到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//标记位旧文件
		db.olderFiles[db.activeFile.Fid] = db.activeFile

		//创建新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//活跃文件的可写位置偏移
	writeOff := db.activeFile.WriteOffset

	//将我们编码后的字节数组写入到活跃文件中(不够放，怎么办？)
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(len)
	//如果配置过写同步磁盘，立即将缓冲区中的数据写入到磁盘中
	var needsync = db.configuration.SyncWrites
	if !needsync && db.configuration.BytesPerSync > 0 && db.bytesWrite >= db.configuration.BytesPerSync {
		//判断当前已写入的字节数是否达到阈值
		needsync = true
	}
	if needsync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.Fid,
		Offset: writeOff,
		Size:   uint32(len),
	}
	//fmt.Printf("fid:%d, offset:%d\n", db.activeFile.Fid, writeOff)
	return pos, nil
}

// 设置当前活跃的数据文件
func (db *DB) setActiveDataFile() error {
	var initialFid uint32 = 0

	//如果已经有活跃文件了， 那么新的活跃文件的Fid递增，否则为初始值0
	if db.activeFile != nil {
		initialFid = db.activeFile.Fid + 1
	}

	//打开活跃文件
	dataFile, err := data.OpenDataFile(db.configuration.DataDir, initialFid, fio.StandardFIO)

	if err != nil {
		return err
	}

	//标记新的活跃文件
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载文件
func (db *DB) loadDataFiles() error {
	//读取目录下所有的文件
	dirEntries, err := os.ReadDir(db.configuration.DataDir)
	if err != nil {
		return err
	}

	//用于存放所有的文件id
	var fds []int
	//遍历这个目录下的所有文件，找到以.data结尾的文件

	for _, entry := range dirEntries {
		//format: 000000001.data
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fd, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return util.ErrDataDirCorrupted
			}
			//取出id存起来
			fds = append(fds, fd)
		}
	}

	//对文件id进行排序
	//因为是追加写入，所以我们约定id递增，文件追加
	sort.Ints(fds)
	db.fds = fds

	//遍历每个id，打开其对应的文件
	for i, fd := range fds {
		ioType := fio.StandardFIO
		if db.configuration.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.configuration.DataDir, uint32(fd), ioType)
		if err != nil {
			return err
		}

		//最后一个文件是活跃文件
		if i+1 == len(fds) {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fd)] = dataFile
		}
	}

	return nil
}

// 从datafile中构造出索引
func (db *DB) LoadIndexFromDataFiles() error {
	//如果没有文件，说明db是空的
	if len(db.fds) == 0 {
		return nil
	}

	// 查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.configuration.DataDir, data.MergeFinFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.configuration.DataDir)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}
	var currentSeqNo = nonTransactionSeqNo
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		//fmt.Println("LoadIndexFromDataFiles", typ)
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
		//fmt.Println("LoadIndexFromDataFiles", ok)
		if !ok {
			panic("failed to udpate index at startup")
		}
	}

	transactionReocrds := make(map[uint64][]*data.TransactionRecord)

	//严格按照时间顺序构建索引
	for _, _fid := range db.fds {
		var fid = uint32(_fid)

		//如果文件id小于nonMergeFileId, 说明在hint文件中已经加载过索引
		if hasMerge && fid < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile

		if fid == db.activeFile.Fid {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}

		var offset int64 = 0
		for {
			//读取一条LogRecord，并得到其大小
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构造索引中的记录
			logRecordPos := &data.LogRecordPos{Fid: fid, Offset: offset, Size: uint32(size)}

			//解析key，拿到对应的事务序列号
			realKey, seqNo := parseLogRecordKeyWithSeq(logRecord.Key)

			//fmt.Printf("LoadIndexFromDataFiles, index size:%d, seqNo:%d, record type:%d\n", db.index.Size(), seqNo, logRecord.Type)

			if seqNo == nonTransactionSeqNo {
				//如果是普通的插入删除(事务序列号为0)，直接处理即可
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//如果事务完成， 对应的seqNo的数据都可以更新到索引中
				if logRecord.Type == data.LogRecordFinished {
					for _, transReocrd := range transactionReocrds[seqNo] {
						updateIndex(transReocrd.Record.Key, transReocrd.Record.Type, transReocrd.Pos)
					}
					delete(transactionReocrds, seqNo)
				} else {
					//待定，先暂存
					logRecord.Key = realKey
					transactionReocrds[seqNo] = append(transactionReocrds[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}

			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//偏移量移动当前LogRecord大小
			offset += size
		}

		//如果是活跃文件，因为我们追加写入需要文件当前的偏移量，这里更新一下
		if fid == db.activeFile.Fid {
			db.activeFile.WriteOffset = offset
		}
	}

	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.configuration.DataDir, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.configuration.DataDir)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

// 将数据文件的IO类型设置为标准文件IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.configuration.DataDir, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.configuration.DataDir, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Stat() *Stat {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := util.DirSize(db.configuration.DataDir)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// 备份数据库， 将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	return util.CopyDir(db.configuration.DataDir, dir, []string{fileLockName})
}
