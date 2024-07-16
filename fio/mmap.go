package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO 文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func (mm *MMap) Read(bytes []byte, offset int64) (int, error) {
	return mm.readerAt.ReadAt(bytes, offset)
}

func (mm *MMap) Write(bytes []byte) (int, error) {
	return 0, nil
}

func (mm *MMap) Sync() error {
	return nil
}

func (mm *MMap) Close() error {
	return mm.readerAt.Close()
}

func (mm *MMap) Size() (int64, error) {
	return int64(mm.readerAt.Len()), nil
}

func NewMMapIOManager(filename string) (*MMap, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, err
	}

	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}
