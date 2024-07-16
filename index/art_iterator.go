package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"github.com/rbongIO/bitcask-go/data"
	"sort"
)

// ARTIterator ART 索引迭代器
type ARTIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否逆序遍历
	values   []*Item // 存储所有的 key和索引信息
}

func (ait *ARTIterator) Rewind() {
	ait.curIndex = 0
}

func (ait *ARTIterator) Seek(key []byte) {
	if ait.reverse {
		ait.curIndex = sort.Search(len(ait.values), func(i int) bool {
			return bytes.Compare(ait.values[i].key, key) <= 0
		})
		return
	}
	ait.curIndex = sort.Search(len(ait.values), func(i int) bool {
		return bytes.Compare(ait.values[i].key, key) >= 0
	})
}

func (ait *ARTIterator) Next() {
	ait.curIndex++
}

func (ait *ARTIterator) Valid() bool {
	return ait.curIndex < len(ait.values)
}

func (ait *ARTIterator) Key() []byte {
	return ait.values[ait.curIndex].key
}

func (ait *ARTIterator) Value() *data.LogRecordPos {
	return ait.values[ait.curIndex].pos
}

func (ait *ARTIterator) Close() {
	ait.values = nil
}

func NewARTIterator(tree goart.Tree, reverse bool) *ARTIterator {
	var idx int
	values := make([]*Item, tree.Size())
	if reverse {
		idx = tree.Size() - 1
	}
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)
	return &ARTIterator{
		reverse: reverse,
		values:  values,
	}
}
