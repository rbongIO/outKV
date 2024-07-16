package index

import (
	"github.com/rbongIO/bitcask-go/data"
	"testing"
)

func TestNewBTreeIterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	t.Log(iter1.Valid())
	bt1.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	bt1.Put([]byte("def"), &data.LogRecordPos{
		Fid:    2,
		Offset: 20,
	})
	bt1.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    3,
		Offset: 30,
	})
	iter2 := bt1.Iterator(false)
	t.Log(iter2.Valid())
	for iter2.Valid() {
		t.Log(iter2.Key(), iter2.Value())
		iter2.Next()
	}
	iter2.Rewind()
	for iter2.Valid() {
		t.Log(iter2.Key(), iter2.Value())
		iter2.Next()
	}
	iter2.Rewind()
	iter2.Seek([]byte("de"))
	t.Log(string(iter2.Key()), iter2.Value())
}
