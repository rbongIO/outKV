package bitcask_go

import (
	"github.com/rbongIO/bitcask-go/data"
	"github.com/rbongIO/bitcask-go/fio"
)

func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(record)
}

// appendLogRecord 将 LogRecord 追加写入到数据文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	if db.activeFile == nil {
		err := db.setActiveDataFile()
		if err != nil {
			return nil, err
		}
	}

	// 将 logRecord 进行编码
	encRecord, size := data.EncodeLogRecord(record)
	// 如果写入的数据已经到达了活跃文件的最大容量，则关闭活跃文件，并创建新的活跃文件
	if db.activeFile.WriteOffset+size > db.options.MaxDataFileSize {
		//先进行持久化，保证已有的记录已被持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前的活跃文件保存为旧的文件
		db.olderFiles[db.activeFile.FileID] = db.activeFile
		err := db.setActiveDataFile()
		if err != nil {
			return nil, err
		}
	}
	writeOffset := db.activeFile.WriteOffset
	err := db.activeFile.Write(encRecord)
	if err != nil {
		return nil, err
	}
	db.bytesWrite += uint64(size)
	//根据用户配置决定是否每次写入都进行持久化
	var needSync = db.options.SyncWrite
	if !needSync && db.options.BytePerSync > 0 && db.bytesWrite > db.options.BytePerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.bytesWrite = 0
	}
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOffset,
		Size:   uint32(size),
	}
	return pos, nil
}

// setActiveDataFile 设置当前活跃的数据文件
// 对共享的 DB实例的访问必须先持有锁
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}
	// 创建新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// Put 写入 Key/Value, 如果 Key 已经存在，则覆盖
func (db *DB) Put(key []byte, value []byte) error {
	//不需要加锁，因为 appendLogRecord 方法中已经加锁

	// 判断 Key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造 LogRecord 结构体
	record := &data.LogRecord{
		Key:   logRecordKeyWithSeqNum(key, nonTransactionSeqNum),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 将 LogRecord 追加写入到数据文件中
	pos, err := db.appendLogRecordWithLock(record)
	if err != nil {
		return err
	}
	// 将 LogRecordPos 更新到内存索引中
	// 更新索引时，索引内部也有锁，所以不用加锁
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}
