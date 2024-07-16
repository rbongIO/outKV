package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常编码一条数据
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("bitcask"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1, n1)
	// value 为空的情况
	rec2 := &LogRecord{
		Key:   []byte("key1"),
		Value: nil,
		Type:  LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res1, n2)
	// 对 deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte("levelDB"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res1, n3)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("bitcask"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	header, headerSize := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
	assert.NotNil(t, header)
	assert.Greater(t, headerSize, int64(5))
	assert.Equal(t, header.recordType, LogRecordNormal)
	assert.Equal(t, header.keySize, uint32(4))
	assert.Equal(t, header.valueSize, uint32(7))
	assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
	{
		rec1 := &LogRecord{
			Key:   []byte("key1"),
			Value: nil,
			Type:  LogRecordNormal,
		}
		res1, n1 := EncodeLogRecord(rec1)
		assert.NotNil(t, res1)
		assert.Greater(t, n1, int64(5))
		assert.Equal(t, n1, int64(4+1+1+4+1))

		header, headerSize := decodeLogRecordHeader(res1)
		assert.NotNil(t, header)
		assert.Greater(t, headerSize, int64(5))
		assert.Equal(t, header.recordType, LogRecordNormal)
		assert.Equal(t, header.keySize, uint32(4))
		assert.Equal(t, header.valueSize, uint32(0))
		assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
	}
	{
		rec1 := &LogRecord{
			Key:   []byte("key1"),
			Value: []byte("bitcask"),
			Type:  LogRecordDeleted,
		}
		res1, n1 := EncodeLogRecord(rec1)
		assert.NotNil(t, res1)
		assert.Greater(t, n1, int64(5))

		header, headerSize := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
		assert.NotNil(t, header)
		assert.Greater(t, headerSize, int64(5))
		assert.Equal(t, header.recordType, LogRecordDeleted)
		assert.Equal(t, header.keySize, uint32(4))
		assert.Equal(t, header.valueSize, uint32(7))
		assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
	}

}

func TestDecodeLogRecord(t *testing.T) {

}

func TestGetLogRecordCrc(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("bitcask"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	header, headerSize := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
	headerBuf := res1[:headerSize]
	crc := getLogRecordCRC(rec1, headerBuf[crc32.Size:])
	assert.Equal(t, crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
	assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
	assert.Equal(t, n1, int64(7+4+7))
	{
		rec1 := &LogRecord{
			Key:  []byte("key1"),
			Type: LogRecordNormal,
		}
		res1, n1 := EncodeLogRecord(rec1)
		header, headerSize := decodeLogRecordHeader(res1)
		headerBuf := res1[:headerSize]
		crc := getLogRecordCRC(rec1, headerBuf[crc32.Size:])
		assert.Equal(t, crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
		assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
		assert.Equal(t, n1, int64(7+4))
	}
	{
		rec1 := &LogRecord{
			Key:   []byte("key1"),
			Value: []byte("bitcask"),
			Type:  LogRecordDeleted,
		}
		res1, n1 := EncodeLogRecord(rec1)
		header, headerSize := decodeLogRecordHeader(res1[:maxLogRecordHeaderSize])
		headerBuf := res1[:headerSize]
		crc := getLogRecordCRC(rec1, headerBuf[crc32.Size:])
		assert.Equal(t, crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
		assert.Equal(t, header.crc, crc32.ChecksumIEEE(res1[crc32.Size:]))
		assert.Equal(t, header.recordType, LogRecordDeleted)
		assert.Equal(t, n1, int64(7+4+7))
	}
}
