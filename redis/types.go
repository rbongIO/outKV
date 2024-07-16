package redis

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"sync"
	"time"
)

type DataType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

const (
	RString DataType = iota + 1
	RHash
	RSet
	RZSet
	RList
)

// DataStructureType 数据结构类型
// db 用来存储转换之后的redis 数据
type DataStructureType struct {
	db *bitcask.DB
	mu *sync.RWMutex
}

// NewDataStructureType 创建一个新的数据结构类型
func NewDataStructureType(opts ...bitcask.OptionFunc) (*DataStructureType, error) {
	db, err := bitcask.Open(opts...)
	if err != nil {
		return nil, err
	}
	return &DataStructureType{db: db, mu: new(sync.RWMutex)}, nil
}

func (d *DataStructureType) Lock() {
	d.mu.Lock()
}
func (d *DataStructureType) UnLock() {
	d.mu.Unlock()
}

// ==================== String 数据结构

func (rds *DataStructureType) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value ：type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = RString
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue, buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入
	return rds.db.Put(key, encValue)
}

func (rds *DataStructureType) Get(key []byte) ([]byte, error) {
	encVal, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	var Rtype = encVal[0]
	if Rtype != RString {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encVal[index:])
	index += n
	if expire != 0 && time.Now().UnixNano() > expire {
		err := rds.Delete(key)
		if err != nil {
			return nil, err
		}
		return nil, bitcask.ErrKeyNotFound
	}
	return encVal[index:], nil
}

func (rds *DataStructureType) Close() error {
	return rds.db.Close()
}
