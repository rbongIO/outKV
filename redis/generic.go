package redis

import (
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"time"
)

func (rds *DataStructureType) Delete(key []byte) error {
	return rds.db.Delete(key)
}
func (rds *DataStructureType) Type(key []byte) (DataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is empty")
	}
	return encValue[0], nil
}

func (rds *DataStructureType) findMetadata(key []byte, dataType DataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == RList {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
