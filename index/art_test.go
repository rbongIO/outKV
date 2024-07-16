package index

import (
	"github.com/rbongIO/bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewAdaptiveRadixTree(t *testing.T) {
	art := NewIndexer(ART, "/tmp", false)
	assert.NotNil(t, art)
	art.Put([]byte("key-1"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	assert.NotNil(t, art.Get([]byte("key-1")))
	pos, deleted := art.Delete([]byte("key-12"))
	assert.Nil(t, pos)
	assert.False(t, deleted)

}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewIndexer(ART, "tmp", false)
	art.Put([]byte("key-1"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-2"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-3"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-4"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-5"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-6"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-7"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-8"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-9"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-10"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-11"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-12"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-13"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-14"), &data.LogRecordPos{
		Fid:    10,
		Offset: 20,
	})
	art.Put([]byte("key-15"), &data.LogRecordPos{
		Fid:    10,
		Offset: 2131212,
	})
	it := art.Iterator(false)
	for it.Valid() {
		t.Log(string(it.Key()), it.Value())
		it.Next()
	}
}
