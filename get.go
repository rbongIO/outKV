package bitcask_go

import (
	"github.com/rbongIO/bitcask-go/data"
)

func (db *DB) Get(key []byte) ([]byte, error) {
	//需要从 DataFile 中读取数据，Datafile 在此期间不能进行修改，所以需要加锁
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断 key 的有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存索引中查找
	recordPos := db.index.Get(key)
	//如果内存索引中没有找到，说明 key 不存在
	if recordPos == nil {
		return nil, ErrKeyNotFound
	}
	//从数据文件中获取具体的数据
	return db.GetValueByPosition(recordPos)
}

// GetValueByPosition 根据 LogRecordPos 获取具体的数据
func (db *DB) GetValueByPosition(recordPos *data.LogRecordPos) ([]byte, error) {
	//根据文件 ID 找到数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileID == recordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[recordPos.Fid]
	}
	//如果数据文件不存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//根据偏移量读取具体的数据
	record, err := dataFile.ReadLogRecord(recordPos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDeleted {
		return nil, nil
	}
	return record.Value, nil
}

func (db *DB) ListKeys() [][]byte {
	db.mu.RLock()
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	size := db.Size()
	db.mu.RUnlock()
	keys := make([][]byte, size)
	for i := 0; iterator.Valid(); iterator.Next() {
		keys[i] = iterator.Key()
		i++
	}
	return keys
}

// Fold 获取所有的数据并执行用户指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		val, err := db.GetValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), val) {
			break
		}
	}
	return nil
}
