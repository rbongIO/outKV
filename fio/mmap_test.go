package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNewMMapIOManager(t *testing.T) {
	_, err := NewIOManager("/tmp/mmap-a.data", MemoryMapIO)
	mmapIO, err := NewMMapIOManager("/tmp/mmap-a.data")

	path := filepath.Join("/tmp", "mmap-a.data")
	defer destroyFile(path)
	mmapIO, err = NewMMapIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)
	t.Log(mmapIO.Size())
	b := make([]byte, 8)
	readLen, err := mmapIO.Read(b, 0)
	assert.Equal(t, err, io.EOF)
	assert.Equal(t, readLen, 0)
	t.Log(string(b))
	t.Log(mmapIO.Size())
	createFile(t, "/tmp/mmap-a.data")
	mmapIO, err = NewMMapIOManager(path)
	assert.Nil(t, err)
	readLen, err = mmapIO.Read(b, 2)
	assert.Nil(t, err)
	assert.Equal(t, readLen, 8)
	t.Log(string(b))
	t.Log(mmapIO.Size())

}

func createFile(t *testing.T, filename string) {
	fio, err := NewIOManager(filename, StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	writeLen, err := fio.Write([]byte("Welcome to China!"))
	assert.Nil(t, err)
	assert.Equal(t, writeLen, len([]byte("Welcome to China!")))
	buf := make([]byte, 8)
	readLen, err := fio.Read(buf, 2)
	assert.Nil(t, err)
	assert.Equal(t, readLen, 8)
	t.Log(string(buf))

	err = fio.Sync()
	assert.Nil(t, err)
	//err = fio.Close()
	assert.Nil(t, err)
}

func destroyFile(filename string) {
	os.Remove(filename)
}
