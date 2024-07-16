package bitcask_go

import (
	"context"
	"github.com/rbongIO/bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

func memStats(ctx context.Context) {
	var m runtime.MemStats
	t := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-t.C:
			runtime.GC()
			runtime.ReadMemStats(&m)
			log.Printf("Alloc = %v TotalAlloc = %v Sys = %vMB NumGC = %v\n hold = %v", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024/1024, m.NumGC, m.HeapReleased/1024)
		case <-ctx.Done():
			return
		}
	}

}
func TestNewFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	go memStats(ctx)
	st := time.Now()
	dir, _ := os.MkdirTemp("", "bitcask-go-New")

	defer cancel()
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false), WithIndexType(ART))
	defer destroyDB(db)
	defer db.Close()
	t.Log(time.Since(st))
	assert.Nil(t, err)
	for i := 0; i < 1000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.GetTestValue(10))
		assert.Nil(t, err)
	}
	it := db.NewIterator(WithPrefix([]byte("bitcask-go-key_{9")))
	st = time.Now()
	for it.Rewind(); it.Valid(); it.Next() {
		//t.Log(string(it.Key()), string(it.Value()))
	}
	//err = db.Fold(func(key []byte, value []byte) bool {
	//	if string(key) == "bitcask-go-key_{999999}" {
	//		t.Log(string(key), string(value))
	//	}
	//	return true
	//})
	//assert.Nil(t, err)
	t.Log(time.Since(st))
	t.Log(db.Size())
}

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-Open")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.GetTestValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false))
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.GetTestValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.GetTestValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	assert.Nil(t, err)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	//opts.DirPath = dir
	//opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	assert.Nil(t, err)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-list-keys")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.GetTestValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.GetTestValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDB_Fold(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-fold")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.GetTestValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.GetTestValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(20))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-sync")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	_, err = Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false))
	assert.Equal(t, ErrDatabaseIsUsing, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(WithDirPath(dir), WithMaxDataFileSize(5<<30), WithSyncWrite(false))
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	err = db2.Close()
	assert.Nil(t, err)
}

func TestDB_Stat(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
	db, err := Open(WithDirPath(dir))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	assert.NotNil(t, stat)
	t.Logf("%+v", stat)
}

func TestDB_Backup(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
	db, err := Open(WithDirPath(dir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 1; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	for i := 1; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	t.Logf("%+v", db.Stat())
	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
	err = db.Backup(backupDir)
	assert.Nil(t, err)

	db2, err := Open(WithDirPath(backupDir), WithMaxDataFileSize(64*1024*1024), WithSyncWrite(false))
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	t.Logf("%+v", db.Stat())
}
func openMMapCreateDB(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	go memStats(ctx)
	now := time.Now()
	db, err := Open(WithDirPath(dir), WithMMapAtStartup(false), WithBytePerSync(1024*1024*32), WithMaxDataFileSize(256*1024*1024), WithSyncWrite(false))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for i := 0; i < 10000000; i++ {
		err = db.Put(utils.GetTestKey(rand.Int()), utils.GetTestValue(128))
	}
	log.Println("put Time", time.Since(now))
	log.Println("db size", db.Size())
}

func destroyMMapDB(dir string) {
	os.RemoveAll(dir)

}

func TestDB_OpenMMap(t *testing.T) {
	dir := "/tmp/bitcask-go"
	//err := os.MkdirAll(dir, os.ModePerm)
	//defer destroyMMapDB(dir)
	//assert.Nil(t, err)
	//openMMapCreateDB(dir)
	now := time.Now()
	db, err := Open(WithDirPath(dir), WithMMapAtStartup(true), WithBytePerSync(1024*1024), WithMaxDataFileSize(256*1024*1024), WithSyncWrite(false))
	assert.Nil(t, err)
	t.Log("open time ", time.Since(now))

	assert.Nil(t, err)
	assert.NotNil(t, db)
}
