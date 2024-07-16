package redis

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"math"
)

func (rds *DataStructureType) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, RZSet)
	if err != nil {
		return false, err
	}
	zk := zSetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}
	var exist bool = true
	//查看是否已经存在
	value, err := rds.db.Get(zk.marshalWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	wb := rds.db.NewWriteBatch()
	if exist {
		if score == decodeFloat64(value) {
			return false, nil
		}
	}
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldzsk := zSetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   decodeFloat64(value),
		}
		_ = wb.Delete(oldzsk.marshalWithMember())
		_ = wb.Delete(oldzsk.marshalWithScore())
	}
	_ = wb.Put(zk.marshalWithScore(), nil)
	_ = wb.Put(zk.marshalWithMember(), encodeFloat64(score))

	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructureType) ZScore(key, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, RZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}
	zk := zSetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	value, err := rds.db.Get(zk.marshalWithMember())
	if err != nil {
		return -1, err
	}
	return decodeFloat64(value), nil
}

type zSetInternalKey struct {
	key     []byte
	version int64
	score   float64
	member  []byte
}

func (zsk *zSetInternalKey) marshalWithScore() []byte {
	buf := make([]byte, len(zsk.key)+len(zsk.member)+8+8+4)
	var index = 0
	//key
	copy(buf[index:], zsk.key)
	index += len(zsk.key)
	//version
	binary.BigEndian.PutUint64(buf[index:], uint64(zsk.version))
	index += 8
	//score
	// 将 float64的 score 转为字节数组
	binary.BigEndian.PutUint64(buf[index:], math.Float64bits(zsk.score))
	index += 8
	//member
	copy(buf[index:], zsk.member)
	index += len(zsk.member)
	// member 长度
	binary.BigEndian.PutUint32(buf[index:], uint32(len(zsk.member)))
	return buf
}

func (zsk *zSetInternalKey) marshalWithMember() []byte {
	buf := make([]byte, len(zsk.key)+len(zsk.member)+8)
	var index = 0
	copy(buf[index:], zsk.key)
	index += len(zsk.key)
	binary.BigEndian.PutUint64(buf[index:], uint64(zsk.version))
	index += 8
	//member
	copy(buf[index:], zsk.member)
	return buf
}

func encodeFloat64(f float64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(f))
	return buf
}
func decodeFloat64(buf []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(buf))
}
