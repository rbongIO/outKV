package fio

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota
	MemoryMapIO
)

const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口，可以接入不同的 IO 类型，目前支持标准文件 IO
type IOManager interface {
	// Read 从文件给定位置读取到对应的数据
	Read([]byte, int64) (int, error)
	// Write 写入字节数组到文件内
	Write([]byte) (int, error)
	// Sync 持久化数据
	Sync() error
	// Close 关闭文件句柄
	Close() error
	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前仅支持 FileIO
func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(filename)
	case MemoryMapIO:
		return NewMMapIOManager(filename)
	default:
		panic("unknown io type")
	}
}
