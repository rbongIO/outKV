package bitcask_go

import (
	"encoding/binary"
	"github.com/rbongIO/bitcask-go/data"
	"github.com/rbongIO/bitcask-go/index"
	"sync"
	"sync/atomic"
)

var txnFinKey = []byte("fin")

const nonTransactionSeqNum uint64 = 0

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写入的数据
}

func (db *DB) NewWriteBatch(opts ...WriteBatchOption) *WriteBatch {
	if db.options.IndexType == index.BPTree && !db.seqNumFileExists && !db.isInitial {
		panic("cannot use write batch,seq no file not exists")
	}
	options := DefaultWriteBatchOptions
	for _, opt := range opts {
		opt(&options)
	}
	return &WriteBatch{
		options:       options,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	rec := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = rec
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在直接返回
	if pos := wb.db.index.Get(key); pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	rec := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = rec
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrBatchNumExceeded
	}
	// 加锁保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 写入数据
	// 1. 获取当前最新的序列号
	seqNum := atomic.AddUint64(&wb.db.seqNum, 1)
	positions := make(map[string]*data.LogRecordPos)
	// 2. 开始写数据到数据文件当中
	for _, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeqNum(record.Key, seqNum),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	// 写一条标识事务完成的数据
	finRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNum(txnFinKey, seqNum),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finRecord); err != nil {
		return err
	}
	//根据配置进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.Sync(); err != nil {
			return err
		}
	}
	// 更新内存索引
	for _, rec := range wb.pendingWrites {
		pos := positions[string(rec.Key)]
		var oldPos *data.LogRecordPos
		switch rec.Type {
		case data.LogRecordNormal:
			oldPos = wb.db.index.Put(rec.Key, pos)
		case data.LogRecordDeleted:
			oldPos, _ = wb.db.index.Delete(rec.Key)
		default:
			panic("unhandled default case")
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	//清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key + seqNum 编码
func logRecordKeyWithSeqNum(key []byte, seqNum uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNum)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析 logRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(encKey []byte) ([]byte, uint64) {
	seqNum, n := binary.Uvarint(encKey)
	key := encKey[n:]
	return key, seqNum
}
