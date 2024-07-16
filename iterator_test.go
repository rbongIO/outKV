package bitcask_go

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	db, err := Open(WithDirPath(os.TempDir()), WithMaxDataFileSize(1<<30), WithSyncWrite(false), WithIndexType(ART))
	assert.Nil(t, err)

	err = db.Put([]byte("test1"), []byte("test1"))
	err = db.Put([]byte("test2"), []byte("test2"))
	err = db.Put([]byte("test3"), []byte("test3"))
	err = db.Put([]byte("test4"), []byte("test4"))
	err = db.Put([]byte("ctest5"), []byte("test5"))
	it := db.NewIterator(WithReverse(true))
	assert.Nil(t, err)
	for it.Valid() {
		t.Log(string(it.Key()), string(it.Value()))
		it.Next()
	}
	it2 := db.NewIterator(WithPrefix([]byte("test")))
	for it2.Valid() {
		t.Log(string(it2.Key()), string(it2.Value()))
		it2.Next()
	}
}
