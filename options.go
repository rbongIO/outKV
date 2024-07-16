package bitcask_go

import "os"

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
	BPTree
)

type Options struct {
	DirPath         string // 数据存储目录
	MaxDataFileSize int64  // 数据文件最大大小
	SyncWrite       bool   // 同步选项
	IndexType       IndexerType
	// 累计写到多少字节后进行持久化
	BytePerSync   uint64
	MMapAtStartup bool
	//达到多少比例后进行合并
	DataFileMergeRatio float32
}

type IteratorOptions struct {
	// Prefix 用于前缀查找，默认为空
	Prefix []byte
	// Reverse 是否逆序遍历，默认为 false
	Reverse bool
}

type WriteBatchOptions struct {
	// MaxBatchNum 最大批量写入数量
	MaxBatchNum uint
	// SyncWrites 是否同步持久化
	SyncWrites bool
}
type OptionFunc func(*Options)
type IteratorOption func(*IteratorOptions)
type WriteBatchOption func(*WriteBatchOptions)

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 1000,
	SyncWrites:  true,
}

func WithMaxBatchNum(maxBatchNum uint) WriteBatchOption {
	return func(o *WriteBatchOptions) {
		o.MaxBatchNum = maxBatchNum
	}
}

func WithSyncWrites(syncWrites bool) WriteBatchOption {
	return func(o *WriteBatchOptions) {
		o.SyncWrites = syncWrites
	}
}
func WithPrefix(prefix []byte) IteratorOption {
	return func(o *IteratorOptions) {
		o.Prefix = prefix
	}
}

func WithReverse(reverse bool) IteratorOption {
	return func(o *IteratorOptions) {
		o.Reverse = reverse
	}
}

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	MaxDataFileSize:    256 * 1024 * 1024, // 256MB
	SyncWrite:          false,
	BytePerSync:        0,
	IndexType:          Btree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

func WithMMapAtStartup(mmapAtStartup bool) OptionFunc {
	return func(o *Options) {
		o.MMapAtStartup = mmapAtStartup
	}
}

func WithDataFileMergeRatio(ratio float32) OptionFunc {
	if ratio < 0 || ratio > 1 {
		panic("invalid merge ratio")
	}
	return func(o *Options) {
		o.DataFileMergeRatio = ratio
	}
}

func WithBytePerSync(bytePerSync uint64) OptionFunc {
	return func(o *Options) {
		o.BytePerSync = bytePerSync
	}
}

func WithDirPath(dirPath string) OptionFunc {
	if dirPath == "" {
		return func(options *Options) {}
	}
	return func(o *Options) {
		o.DirPath = dirPath
	}
}

func WithMaxDataFileSize(maxDataFileSize int64) OptionFunc {
	return func(o *Options) {
		o.MaxDataFileSize = maxDataFileSize
	}
}

func WithSyncWrite(syncWrites bool) OptionFunc {
	return func(o *Options) {
		o.SyncWrite = syncWrites
	}
}

func WithIndexType(indexType IndexerType) OptionFunc {
	return func(o *Options) {
		o.IndexType = indexType
	}
}
