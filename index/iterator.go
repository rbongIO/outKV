package index

import "github.com/rbongIO/bitcask-go/data"

// Iterator 索引迭代器接口
type Iterator interface {
	// Rewind 重置迭代器回到起始位置
	Rewind()
	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据这个 key 开始遍历
	Seek(key []byte)
	// Next 移动到下一个 key
	Next()
	// Valid 判断当前迭代器是否有效（遍历完会失效），用于退出遍历
	Valid() bool
	// Key 获取当前位置的 key
	Key() []byte
	// Value 获取当前位置的 value
	Value() *data.LogRecordPos
	// Close 关闭迭代器，释放相应资源
	Close()
}
