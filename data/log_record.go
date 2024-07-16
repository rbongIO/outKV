package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valSize
// 4 + 1 + 5 + 5 = 15
const (
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5
)

// LogRecord 写入到数据文件中的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
	Size   uint32 // 在磁盘上的大小
}

type logRecordHeader struct {
	crc        uint32        // crc 校验码
	recordType LogRecordType // 记录类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// TransactionRecord 事务记录结构体
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// Marshal 序列化 LogRecord 结构体ß
func (lr *LogRecord) Marshal() ([]byte, error) {
	return nil, nil
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
// +-----------+------------------+-------+----------+---------+----------+
// ｜crc(4byte)｜recordType(1byte)｜keySize｜valueSize｜keyBytes｜valueBytes｜
// +-----------+------------------+-------+----------+---------+----------+
// ｜  4byte   ｜       1byte      ｜ 变长（最大5byte）｜ 变长 （最大5byte）  ｜ 变长｜ 变长｜
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	//从第五个字节开始写
	header[4] = lr.Type
	var index = 5
	// 5字节之后，存储的是 key 和 value 的长度信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))
	var size = index + len(lr.Key) + len(lr.Value)
	encBytes := make([]byte, size)
	//将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key/value 的部分拷贝过来
	copy(encBytes[index:], lr.Key)
	copy(encBytes[index+len(lr.Key):], lr.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

func decodeLogRecordHeader(b []byte) (*logRecordHeader, int64) {
	if len(b) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(b[:4]),
		recordType: b[4],
	}
	var index = 5
	// 读取 key 和 value 的长度
	keySize, n := binary.Varint(b[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(b[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

func DecodeLogRecord(b []byte) (*LogRecord, error) {
	// 0. 校验 crc

	// 1. 读取 Key 和 Value 的长度
	// 2. 读取数据
	// 3. 返回数据
	return nil, nil
}

func getLogRecordCRC(lr *LogRecord, header []byte) (crc uint32) {
	if lr == nil {
		return
	}
	crc = crc32.ChecksumIEEE(header)
	// 计算 key 和 value 的 crc获取总的 crc 的值
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return
}

func (p *LogRecordPos) Marshal() []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64+binary.MaxVarintLen32)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(p.Fid))
	index += binary.PutUvarint(buf[index:], uint64(p.Offset))
	index += binary.PutUvarint(buf[index:], uint64(p.Size))
	return buf[:index]
}
func DecodeLogRecordPos(b []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Uvarint(b)
	index += n
	offset, n := binary.Uvarint(b[index:])
	index += n
	size, n := binary.Uvarint(b[index:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: int64(offset),
		Size:   uint32(size),
	}
}
