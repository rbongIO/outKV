package redis

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
)

func (rds *DataStructureType) HSet(key, filed, value []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RHash)
	if err != nil {
		return false, err
	}

	// 构造hash 结构的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}
	encKey := hk.marshal()

	// 先查找数据是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	wb := rds.db.NewWriteBatch()
	// 不存在需要更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructureType) HGet(key, filed []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, RHash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	// 构造hash 结构的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}

	// 获取数据
	return rds.db.Get(hk.marshal())
}

func (rds *DataStructureType) HDel(key, filed []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RHash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}
	encKey := hk.marshal()
	// 先查找数据是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	if !exist {
		return false, nil
	}
	wb := rds.db.NewWriteBatch()
	_ = wb.Delete(encKey)
	meta.size--
	_ = wb.Put(key, meta.encode())
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil

}

type hashInternalKey struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKey) marshal() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.filed))
	var index = 0
	copy(buf[index:], hk.key)
	index += len(hk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(hk.version))
	index += 8
	copy(buf[index:], hk.filed)
	return buf
}
