package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMeta   = 2 * binary.MaxVarintLen64
	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType DataType // 数据类型
	expire   int64    // 过期时间
	version  int64    // 版本号
	size     uint32   // 数据大小
	head     uint64   // List 数据结构专有
	tail     uint64   // List 数据结构专有
}

func (md *metadata) encode() []byte {
	var buf []byte
	if md.dataType == RList {
		buf = make([]byte, maxMetadataSize+extraListMeta)
	} else {
		buf = make([]byte, maxMetadataSize)
	}
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	binary.LittleEndian.PutUint32(buf[index:], md.size)
	index += 4
	if md.dataType == RList {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetadata(b []byte) *metadata {
	md := &metadata{}
	var n int
	md.dataType = b[0]
	var index = 1
	md.expire, n = binary.Varint(b[index:])
	index += n
	md.version, n = binary.Varint(b[index:])
	index += n
	md.size = binary.LittleEndian.Uint32(b[index:])
	index += 4
	if md.dataType == RList {
		md.head, n = binary.Uvarint(b[index:])
		index += n
		md.tail, _ = binary.Uvarint(b[index:])
	}
	return md
}
