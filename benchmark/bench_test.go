package benchmark

import (
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"github.com/rbongIO/bitcask-go/utils"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *bitcask.DB
var listKeys [][]byte

func init() {
	var err error
	dir, _ := os.MkdirTemp("/tmp", "bitcask-go")
	db, err = bitcask.Open(bitcask.WithDirPath(dir), bitcask.WithIndexType(bitcask.ART))
	//log.Println(dir)
	if err != nil {
		panic(err)
	}
	//for i := 0; i < 100000; i++ {
	//	err := db.Put(utils.GetTestKey(rand.Int()), []byte(utils.GetTestValue(24)))
	//	if err != nil {
	//		log.Printf("Put() error = %v", err)
	//		return
	//	}
	//}
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	//b.Logf("%+v", db.Stat())
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), []byte(utils.GetTestValue(1024)))
		if err != nil {
			b.Errorf("Put() error = %v", err)
			return
		}
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte(utils.GetTestValue(1024)))
		if err != nil {
			log.Printf("Put() error = %v", err)
			return
		}
	}
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
			b.Errorf("Get() error = %v", err)
			return
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte(utils.GetTestValue(1024)))
		if err != nil {
			log.Printf("Put() error = %v", err)
			return
		}
	}
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		if err != nil {
			b.Errorf("Delete() error = %v", err)
			return
		}
	}
}
