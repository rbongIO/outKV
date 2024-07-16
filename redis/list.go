package redis

import (
	"encoding/binary"
	bitcask "github.com/rbongIO/bitcask-go"
)

func (rds *DataStructureType) LPush(key, value []byte) (uint32, error) {
	return rds.pushInner(key, value, true)
}
func (rds *DataStructureType) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}
func (rds *DataStructureType) RPush(key, value []byte) (uint32, error) {
	return rds.pushInner(key, value, false)

}
func (rds *DataStructureType) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *DataStructureType) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, RList)
	if err != nil {
		return 0, err
	}
	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}
	wb := rds.db.NewWriteBatch()
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.marshal(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *DataStructureType) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, RList)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, bitcask.ErrKeyNotFound
	}
	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}
	element, err := rds.db.Get(lk.marshal())
	if err != nil {
		return nil, err
	}
	wb := rds.db.NewWriteBatch()
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(lk.marshal())
	if err = wb.Commit(); err != nil {
		return nil, err
	}
	return element, nil
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) marshal() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0
	copy(buf[index:], lk.key)
	index += len(lk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(lk.version))
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], lk.index)
	return buf
}
