package index

import (
	"github.com/rbongIO/bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	tree := NewBPlusTree(os.TempDir(), false)
	defer func() {
		tree.Close()
		os.Remove(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()
	tree.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	tree.Put([]byte("def"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	tree.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    3,
		Offset: 300,
	})
}
func TestBPlusTree_Get(t *testing.T) {
	//path := filepath.Join("/tmp")
	path := os.TempDir()
	tree := NewBPlusTree(path, false)
	defer func() {
		tree.Close()
		os.Remove(filepath.Join(path, bptreeIndexFileName))
	}()
	tree.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	tree.Put([]byte("def"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	tree.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    3,
		Offset: 300,
	})
	pos := tree.Get([]byte("abc"))
	assert.NotNil(t, pos)
	t.Log(pos)
	pos = tree.Get([]byte("def"))
	assert.NotNil(t, pos)
	t.Log(pos)
	pos = tree.Get([]byte("hij"))
	assert.NotNil(t, pos)
	t.Log(pos)
	pos = tree.Get([]byte("klm"))
	assert.Nil(t, pos)
	t.Log(pos)
}

func TestBPlusTree_Delete(t *testing.T) {
	//path := filepath.Join("/tmp")
	path := os.TempDir()
	tree := NewIndexer(BPTree, path, false)
	defer func() {
		tree.Close()
		os.Remove(filepath.Join(path, bptreeIndexFileName))
	}()
	//defer func() {
	//	tree.Close()
	//	os.Remove(filepath.Join(path, bptreeIndexFileName))
	//}()
	tree.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	tree.Put([]byte("def"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	tree.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    3,
		Offset: 300,
	})
	pos := tree.Get([]byte("abc"))
	assert.NotNil(t, pos)
	t.Log(pos)

	pos, res := tree.Delete([]byte("abc"))
	assert.True(t, res)
	assert.NotNil(t, pos)
	pos = tree.Get([]byte("abc"))
	assert.Nil(t, pos)
	t.Log(pos)
	pos, res = tree.Delete([]byte("abc"))
	assert.False(t, res)
	assert.Nil(t, pos)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := os.TempDir()
	t.Log(path)

	tree := NewBPlusTree(path, false)
	defer func() {
		tree.Close()
		os.Remove(filepath.Join(path, bptreeIndexFileName))
	}()
	res := tree.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	t.Log(res)
	assert.Nil(t, res)
	res = tree.Put([]byte("def"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	assert.Nil(t, res)
	res = tree.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    3,
		Offset: 300,
	})
	assert.Nil(t, res)
	res = tree.Put([]byte("java"), &data.LogRecordPos{
		Fid:    4,
		Offset: 400,
	})
	assert.Nil(t, res)
	res = tree.Put([]byte("java"), &data.LogRecordPos{
		Fid:    5,
		Offset: 500,
	})
	assert.NotNil(t, res)
	it := tree.Iterator(true)

	for it.Rewind(); it.Valid(); it.Next() {
		t.Log(string(it.Key()), it.Value())
	}
	t.Log("forward")
	it.Close()
	t.Log("reverse")
}
