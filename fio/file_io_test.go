package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewIOManager(filepath.Join("/tmp", "a.data"))
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
	err = fio.Close()
	assert.Nil(t, err)
	err = os.Remove(filepath.Join("/tmp", "a.data"))
	if err != nil {
		t.Fatal(err)
	}
}
