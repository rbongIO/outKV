package index

import (
	"github.com/google/btree"
	"github.com/rbongIO/bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	pos := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.Nil(t, pos)

	pos = bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	assert.Nil(t, pos)
	pos = bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    3,
		Offset: 300,
	})
	assert.NotNil(t, pos)
}

func putToBtree(bt *BTree) {
	bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    12,
		Offset: 200,
	})
	bt.Put([]byte("def"), &data.LogRecordPos{
		Fid:    22,
		Offset: 300,
	})
	bt.Put([]byte("hij"), &data.LogRecordPos{
		Fid:    32,
		Offset: 400,
	})
}

func TestBTree_Get(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		key []byte
	}
	bt := NewBTree()
	f := fields{
		tree: bt.tree,
		lock: bt.lock,
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *data.LogRecordPos
	}{
		{"abc", f, args{key: []byte("abc")}, &data.LogRecordPos{
			Fid:    12,
			Offset: 200,
		}},
		{"def", f, args{key: []byte("def")}, &data.LogRecordPos{
			Fid:    22,
			Offset: 300,
		}},
		{"hij", f, args{key: []byte("hij")}, &data.LogRecordPos{
			Fid:    32,
			Offset: 400,
		}},
	}
	putToBtree(bt)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			assert.Equalf(t, tt.want, bt.Get(tt.args.key), "Get(%v)", tt.args.key)
		})
	}
	pos := bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.NotNil(t, pos)
	logRec := bt.Get([]byte("abc"))
	t.Log(logRec)
	assert.Equal(t, &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	}, logRec)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	putToBtree(bt)
	pos := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.Nil(t, pos)
	logRec := bt.Get(nil)
	t.Log(logRec)
	pos, ok := bt.Delete(nil)
	assert.NotNil(t, pos)
	assert.True(t, ok)
}
