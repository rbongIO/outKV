package data

import (
	"github.com/rbongIO/bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDataFile(dataFile *DataFile) {
	if dataFile != nil {
		err := os.Remove(GetDataFileName(os.TempDir(), dataFile.FileID))
		if err != nil {
			panic(err)
		}
	}

}

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	defer destroyDataFile(dataFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	dataFile1, err := OpenDataFile(os.TempDir(), 110, fio.StandardFIO)
	defer destroyDataFile(dataFile1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	dataFile2, err := OpenDataFile(os.TempDir(), 210, fio.StandardFIO)
	defer destroyDataFile(dataFile2)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer destroyDataFile(dataFile)
	err = dataFile.Write([]byte("\n1abcdefgKey"))
	assert.Nil(t, err)
	err = dataFile.Write([]byte("\n2abcdefgKey"))
	assert.Nil(t, err)
	err = dataFile.Write([]byte("\r\n3abcdefgKey"))
	assert.Nil(t, err)
}
func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer destroyDataFile(dataFile)
	err = dataFile.Write([]byte("\n1abcdefgKey"))
	assert.Nil(t, err)
	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	defer destroyDataFile(dataFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("\n1abcdefgKey"))
	assert.Nil(t, err)
	err = dataFile.Sync()
	assert.Nil(t, err)
	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Write2(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1001, fio.StandardFIO)
	defer destroyDataFile(dataFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	rec1 := LogRecord{
		Key:   []byte("testKey2"),
		Value: []byte("mongo"),
		Type:  LogRecordNormal,
	}
	encRecord1, n1 := EncodeLogRecord(&rec1)
	assert.NotNil(t, encRecord1)
	assert.Greater(t, n1, int64(5))

	err = dataFile.Write(encRecord1)
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {

	dataFile, err := OpenDataFile(os.TempDir(), 1001, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer destroyDataFile(dataFile)
	rec1 := &LogRecord{
		Key:   []byte("testKey1"),
		Value: []byte("bitcask"),
		Type:  LogRecordNormal,
	}
	encRecord1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, encRecord1)
	assert.Greater(t, n1, int64(5))

	err = dataFile.Write(encRecord1)
	var index int64 = 0
	record1, n1, err := dataFile.ReadLogRecordWithSize(index)
	assert.Nil(t, err)
	assert.Equal(t, rec1, record1)
	assert.Equal(t, int64(5+2+8+7), n1)
	index += n1
	rec2 := &LogRecord{
		Key:   []byte("testKey2"),
		Value: []byte("mongo"),
		Type:  LogRecordNormal,
	}
	encRecord2, n1 := EncodeLogRecord(rec2)
	assert.NotNil(t, encRecord2)
	assert.Greater(t, n1, int64(5))

	err = dataFile.Write(encRecord2)
	record, n, err := dataFile.ReadLogRecordWithSize(index)
	assert.Nil(t, err)
	assert.Equal(t, rec2, record)
	assert.Equal(t, int64(5+2+8+5), n)
}
