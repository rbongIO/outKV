package index

import (
	"github.com/google/btree"
	"github.com/rbongIO/bitcask-go/data"
	"sync"
)

// BTree 索引，主要封装了 google 的 btree lib
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()

	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, true
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Size() int64 {
	return int64(bt.tree.Len())
}

func (bt *BTree) Close() error {
	return nil
}
