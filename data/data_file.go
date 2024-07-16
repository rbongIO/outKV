package data

import (
	"errors"
	"fmt"
	"github.com/rbongIO/bitcask-go/fio"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

var ErrInvalidCRC = errors.New("invalid crc")

const (
	DataFileNameSuffix = ".data"
	HintFileName       = "hint-index"
	MergeFinishedName  = "hint-finished"
	SeqNumFileName     = "sequence-num"
)

// DataFile 数据文件结构体
type DataFile struct {
	Filepath    string
	FileID      uint32        // 文件 ID
	WriteOffset int64         // 当前文件写入位置
	IOManager   fio.IOManager // io 读写操作
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileID uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileID)
	return NewDataFile(fileName, fileID, ioType)
}

// OpenHintFile 打开 hint 文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return NewDataFile(fileName, 0, fio.StandardFIO)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedName)
	return NewDataFile(fileName, 0, fio.StandardFIO)
}
func MergeFinished(dirPath string) error {
	return os.Rename(filepath.Join(dirPath, HintFileName), filepath.Join(dirPath, MergeFinishedName))
}

func OpenSeqNumFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, SeqNumFileName)
	return NewDataFile(filename, 0, fio.StandardFIO)
}

func GetDataFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileID, DataFileNameSuffix))
}

func NewDataFile(fileName string, fileID uint32, ioType fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		Filepath:    fileName,
		FileID:      fileID,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Write(v []byte) error {
	writeLen, err := df.IOManager.Write(v)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(writeLen)
	return nil
}

// WriteLogRecord 写入 索引信息 到Hint文件
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	rec := &LogRecord{
		Key:   key,
		Value: pos.Marshal(),
	}
	encRec, _ := EncodeLogRecord(rec)
	return df.Write(encRec)
}

func (df *DataFile) Read(offset int64) ([]byte, error) {
	return nil, nil
}
func (df *DataFile) ReadLogRecordWithSize(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	//如果读取的最大 header 长度已经超过了文件长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}
	//读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//解析 Header 信息，获得 header 结构体和 header 实际大小
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// header == nil 说明读取到文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	// header.crc == 0 && header.keySize == 0 && header.valueSize == 0 说明是空记录
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 计算整个记录的大小
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize
	var record = &LogRecord{}
	//开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//解析出 key 和 value
		record.Key = kvBuf[:keySize]
		record.Value = kvBuf[keySize:]
		record.Type = header.recordType
	}
	//校验 CRC 是否正确
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return record, recordSize, nil
}

// ReadLogRecord 根据 offset 从数据文件当中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	rec, _, err := df.ReadLogRecordWithSize(offset)
	return rec, err
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) SetIOManager(ioType fio.FileIOType) error {
	if err := df.IOManager.Close(); err != nil {
		return nil
	}
	manager, err := fio.NewIOManager(df.Filepath, ioType)
	if err != nil {
		return err
	}
	df.IOManager = manager
	return nil
}
