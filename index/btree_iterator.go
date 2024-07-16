package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/rbongIO/bitcask-go/data"
	"sort"
)

// BTreeIterator BTree 索引迭代器
type BTreeIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否逆序遍历
	values   []*Item // 存储所有的 key和索引信息
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	// getValues 用于遍历 btree 中的所有元素，并将其存储到 values 中
	getValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(getValues)
	} else {
		tree.Ascend(getValues)
	}

	return &BTreeIterator{
		curIndex: 0,
		values:   values,
		reverse:  reverse,
	}
}

func (bti *BTreeIterator) Rewind() {
	bti.curIndex = 0
}

func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
		return
	}
	bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
		return bytes.Compare(bti.values[i].key, key) >= 0
	})
}

func (bti *BTreeIterator) Next() {
	bti.curIndex++
}

func (bti *BTreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

func (bti *BTreeIterator) Close() {
	bti.values = nil
}
