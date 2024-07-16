package index

import (
	"github.com/rbongIO/bitcask-go/data"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree  BoltDB 索引
type BPlusTree struct {
	tree *bolt.DB
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldItem []byte
	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldItem = bucket.Get(key)
		err := bucket.Put(key, pos.Marshal())
		return err
	}); err != nil {
		panic("failed to put value into bptree")
	}
	if len(oldItem) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldItem)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		val := bucket.Get(key)
		if len(val) != 0 {
			pos = data.DecodeLogRecordPos(val)
		}
		return nil
	}); err != nil {
		panic("failed to get value from bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	//var ok bool
	var oldItem []byte
	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldItem = bucket.Get(key); len(oldItem) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value into bptree")
	}
	if len(oldItem) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldItem), true
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Size() int64 {
	var size int
	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size from bptree")
	}
	return int64(size)
}

// NewBPlusTree 创建一个新的 B+ 树索引
func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bolt.DefaultOptions
	opts.NoSync = syncWrite
	bptree, err := bolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic(err)
	}
	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}
	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}
