package index

import (
	"github.com/rbongIO/bitcask-go/data"
	bolt "go.etcd.io/bbolt"
)

type bptreeIterator struct {
	tx       *bolt.Tx
	cursor   *bolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func (bpt *bptreeIterator) Rewind() {
	if bpt.reverse {
		bpt.curKey, bpt.curValue = bpt.cursor.Last()
	} else {
		bpt.curKey, bpt.curValue = bpt.cursor.First()
	}
}

func (bpt *bptreeIterator) Seek(key []byte) {
	bpt.curKey, bpt.curValue = bpt.cursor.Seek(key)
}

func (bpt *bptreeIterator) Next() {
	if bpt.reverse {
		bpt.curKey, bpt.curValue = bpt.cursor.Prev()
	} else {
		bpt.curKey, bpt.curValue = bpt.cursor.Next()
	}
}

func (bpt *bptreeIterator) Valid() bool {
	return bpt.curKey != nil
}

func (bpt *bptreeIterator) Key() []byte {
	return bpt.curKey
}

func (bpt *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpt.curValue)
}

func (bpt *bptreeIterator) Close() {
	_ = bpt.tx.Rollback()
}

func newBptreeIterator(tree *bolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bptC := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bptC.Rewind()
	return bptC
}
