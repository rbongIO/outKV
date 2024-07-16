package index

import (
	goart "github.com/plar/go-adaptive-radix-tree"
	"github.com/rbongIO/bitcask-go/data"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldPos, _ := art.tree.Insert(key, pos)
	if oldPos == nil {
		return nil
	}
	return oldPos.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	pos, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return pos.(*data.LogRecordPos)

}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldItem, deleted := art.tree.Delete(key)
	if oldItem == nil {
		return nil, deleted
	}
	return oldItem.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int64 {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return int64(art.tree.Size())
}

// NewAdaptiveRadixTree 初始化 ART
func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}
