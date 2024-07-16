package bitcask_go

import (
	"github.com/rbongIO/bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	db, err := Open(WithDirPath("/Users/bongio/go/src/github.com/rbongIO/bitcask-go"), WithMaxDataFileSize(5<<30), WithSyncWrite(false), WithIndexType(ART))
	assert.Nil(t, err)
	defer db.Close()
	log.Println(db.index.Get([]byte("testKey1")))
	wb := db.NewWriteBatch(WithSyncWrites(true), WithMaxBatchNum(100))
	assert.NotNil(t, wb)
	key1, value1 := []byte("testKey1"), []byte("testValue1")
	err = wb.Put(key1, value1)
	assert.Nil(t, err)
	err = wb.Delete([]byte("abc"))
	assert.Nil(t, err)
	key2, value2 := utils.GetTestKey(rand.Int()), utils.GetTestValue(24)
	err = wb.Put(key2, value2)
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	val, err := db.Get(key1)
	it := db.NewIterator()
	for it.Valid() {
		t.Log(string(it.Key()), string(it.Value()))
		it.Next()
	}
	assert.Nil(t, err)
	assert.Equal(t, value1, val)
}
