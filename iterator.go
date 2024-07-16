package bitcask_go

import (
	"bytes"
	"github.com/rbongIO/bitcask-go/index"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化 DB 迭代器
func (db *DB) NewIterator(opts ...IteratorOption) *Iterator {
	options := DefaultIteratorOptions
	for _, opt := range opts {
		opt(&options)
	}
	it := &Iterator{
		indexIter: db.index.Iterator(options.Reverse),
		db:        db,
		options:   options,
	}

	if options.Prefix != nil {
		it.skipToNext()
	}

	return it

}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() []byte {
	pos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	val, _ := it.db.GetValueByPosition(pos)
	return val

}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	if len(it.options.Prefix) == 0 {
		return
	}
	for it.Valid() {
		if bytes.HasPrefix(it.Key(), it.options.Prefix) {
			break
		}
		it.Next()
	}
}
