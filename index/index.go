package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/rbongIO/bitcask-go/data"
)

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	//自适应基数索引
	ART
	BPTree
)

// Indexer 抽象索引接口，后续需要替换其他索引数据结构时，只需实现该接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	Iterator(reverse bool) Iterator
	Size() int64
	Close() error
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

func NewIndexer(typ IndexerType, dirPath string, syncWrite bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// TODO: 实现 ART 索引
		return NewAdaptiveRadixTree()
	case BPTree:
		return NewBPlusTree(dirPath, syncWrite)
	default:
		panic("unsupported indexer type")
	}

}
