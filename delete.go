package bitcask_go

import "github.com/rbongIO/bitcask-go/data"

// Delete 根据 key 删除对应数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 在内存索引中查找，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造 LogRecord，标识其被删除
	record := &data.LogRecord{Key: logRecordKeyWithSeqNum(key, nonTransactionSeqNum), Type: data.LogRecordDeleted}
	//写入数据文件中
	pos, err := db.appendLogRecordWithLock(record)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)
	pos, ok := db.index.Delete(key)

	if !ok {
		return ErrIndexUpdateFailed
	}
	if pos != nil {
		db.reclaimSize += int64(pos.Size)
	}
	return nil
}
