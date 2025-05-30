package bitcask

import (
	"Bitcask-go/config"
	"Bitcask-go/data"
	"Bitcask-go/index"
	"Bitcask-go/util"
	"sync"
)

type DB struct {
	mutex         *sync.RWMutex             //读写锁，保证线程安全
	activeFile    *data.DataFile            //当前活跃的文件
	olderFiles    map[uint32]*data.DataFile //旧文件，只能用于读
	configuration config.Configuration      //用户配置项
	index         index.Indexer             //内存索引, [key, LogRecordPos]
}

// 通过配置项构造一个DB
func Open(cfg Configuration) (*DB, error) {
	if err := config.checkCfg(cfg); err != nil {
		return nil, err
	}

	//如果用户配置的文件目录不存在，则创建这个目录
	if _, err := os.Stat(cfg.DataDir); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化 DB
	db := &DB {
		mutex : new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		configuration: cfg,
		index: index.NewIndexer(cfg.indexerType),
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
}

// 写入数据到DB中， key不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return util.ErrKeyIsEmpty
	}

	//构造一个LogRecord，准备写入到磁盘的数据文件中
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	//追加写入到磁盘的活跃文件中
	pos, err := db.appendLogRecord(log_record)

	if err != nil {
		return err
	}

	//写入到磁盘中之后，更新内存中的索引(TODO, 这里会不会写磁盘成功，更新内存失败，造成数据不一致)
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

	//对数据进行编码，返回byte[]
	encRecord, len := data.EncodeLogRecord(logRecord)

	//如果写入的文件已经不能够容纳新的记录，则将当前活跃文件关闭，并创建一个新的活跃文件
	if db.activeFile.WriteOffset + len > db.configuration.DataFileMaxSize {
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

	//如果配置过写同步磁盘，立即将缓冲区中的数据写入到磁盘中
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

	//如果已经有活跃文件了， 那么新的活跃文件的Fid递增，否则为初始值0
	if db.activeFile != nil {
		initialFid = db.activeFile.Fid + 1
	}

	//打开活跃文件
	dataFile, err := data.OpenDataFile(db.configuration.DataDir, initialFid)

	if err != nil {
		return err
	}

	//标记新的活跃文件
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载文件
func (db *DB)loadDataFiles() error {
	dirEntries, err := os.ReadDir(dp.config.DataDir)
	if err != nil {
		return err
	}

	var fds []int
	//遍历这个目录下的所有文件，找到以.data结尾的文件

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fd, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return util.ErrDataDirCorrupted
			}

			fds = append(fds, fd)
		}
	}

	//对文件id进行排序
	sort.Ints(fds)

	//遍历每个id，打开其对应的文件
	for i, fd := range fds {
		dataFile, err := data.OpenDataFile(db.config.DirPath, uint32(fd))
		if err != nil {
			 return err
		}

		//最后一个文件是活跃文件
		if i + 1 == len(fds) {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fd] = dataFile
		}
	}

	return nil
}

