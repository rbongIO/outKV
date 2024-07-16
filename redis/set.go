package redis

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
)

func (rds *DataStructureType) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RSet)
	if err != nil {
		return false, err
	}
	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.marshal()
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		wb := rds.db.NewWriteBatch()
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (rds *DataStructureType) SIsMember(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.marshal()
	_, err = rds.db.Get(encKey)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}
func (rds *DataStructureType) SRem(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, RSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.marshal()
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	wb := rds.db.NewWriteBatch()
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) marshal() []byte {
	buf := make([]byte, len(sk.key)+8+len(sk.member)+4)
	var index = 0
	copy(buf[index:], sk.key)
	index += len(sk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(sk.version))
	index += 8
	copy(buf[index:], sk.member)
	index += len(sk.member)
	// member 长度
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))
	return buf
}
