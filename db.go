package bitcask_go

import (
	"fmt"
	"github.com/gofrs/flock"
	"github.com/rbongIO/bitcask-go/data"
	"github.com/rbongIO/bitcask-go/fio"
	"github.com/rbongIO/bitcask-go/index"
	"github.com/rbongIO/bitcask-go/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	mu               *sync.RWMutex
	fileIds          []int                     //只用于加载索引
	activeFile       *data.DataFile            //当前活跃的数据文件，可以用于写入
	olderFiles       map[uint32]*data.DataFile //已经关闭的数据文件，用于读取
	options          Options
	index            index.Indexer
	seqNum           uint64 //十五序列号
	isMerging        bool   //是否正在合并数据文件
	seqNumFileExists bool
	isInitial        bool
	fileLock         *flock.Flock //文件锁保证多进程之间的互斥访问
	bytesWrite       uint64       // 当前累计写了多少
	reclaimSize      int64
}
type Stat struct {
	KeyNum          uint  //键的数量
	DataFileNum     uint  //数据文件数量
	ReclaimableSize int64 //可回收的大小
	DiskSize        int64 //磁盘大小
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

const seqNumKey = "seqNum"
const fileLockName = "flock"

// Open 打开数据库，并返回一个数据库实例
func Open(opts ...OptionFunc) (*DB, error) {
	o := DefaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	var isInitial bool
	//判断目录是否存在，如果不存在需要去创建目录
	if _, err := os.Stat(o.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(o.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(o.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(o.DirPath)
	if err != nil {
		return nil, err
	}
	//如果目录下没有文件，说明数据库是空的
	if len(entries) == 0 {
		isInitial = true
	}
	//初始化 DB 实例结构体
	db := &DB{
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		options:    o,
		index:      index.NewIndexer(o.IndexType, o.DirPath, o.SyncWrite),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//B+树中不需要从文件中加载索引
	if o.IndexType != index.BPTree {
		//从 hint 文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		//从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		//重置 MMAP 为标准 IO
		if o.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}
	}
	if o.IndexType == index.BPTree {
		if err := db.loadSeqNum(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}
	return db, nil
}

// 加载数据文件的方法
func (db *DB) loadDataFiles() error {
	dirEntry, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历所有文件，获取以.data 结尾的数据文件
	for _, entry := range dirEntry {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 000001.data -> 000001
			splitName := strings.Split(entry.Name(), ".")
			if len(splitName) != 2 {
				continue
			}
			fileId, err := strconv.Atoi(splitName[0])
			//数据目录可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	if len(fileIds) == 0 {
		file, err := data.OpenDataFile(db.options.DirPath, 0, fio.StandardFIO)
		if err != nil {
			return err
		}
		fileIds = append(fileIds, 0)
		db.activeFile = file
		return nil
	}
	//对文件进行排序，从小到大一次加载数据文件
	sort.Ints(fileIds)
	db.fileIds = fileIds
	var ioType fio.FileIOType = fio.StandardFIO
	if db.options.MMapAtStartup {
		ioType = fio.MemoryMapIO
	}
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			//最后一个数据文件作为活跃文件
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}
	return nil
}

// Sync 同步数据文件
func (db *DB) Sync() error {
	return db.activeFile.Sync()
}

// 从数据文件中加载索引
// 遍历文件中所有记录并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明数据库是空的直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	// 查看是否发生过 merge
	hasMerge, nonMergeFileID := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		hasMerge = true
		nonMergeFileID, err = db.getNonMergeFileID(mergeFinFileName)
		if err != nil {
			return err
		}
	}
	// 暂存事务的集合
	var curSeqNum = nonTransactionSeqNum
	var txns = make(map[uint64][]*data.TransactionRecord)
	// 遍历所有文件 id，处理文件中的记录
	for i, fid := range db.fileIds {
		// 如果发生过 merge，只加载 merge 之后的文件
		if hasMerge && uint32(fid) < nonMergeFileID {
			continue
		}
		var dataFile *data.DataFile
		var fileId = uint32(fid)
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			rec, size, err := dataFile.ReadLogRecordWithSize(offset)

			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			rec.Value = nil
			//构建内存索引并保存
			logRecPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}
			// 解析 key，拿到事务序列号
			key, seqNum := parseLogRecordKey(rec.Key)
			if seqNum == nonTransactionSeqNum {
				// 非事务操作，直接更新内存索引
				ok := db.updateIndex(key, rec.Type, logRecPos)
				if !ok {
					//return ErrIndexUpdateFailed
					panic(ErrIndexUpdateFailed)
				}

			} else {
				// 事务完成，对应的 seqNUm 的数据可以更新到内存索引中
				if rec.Type == data.LogRecordTxnFinished {
					for _, txnRec := range txns[seqNum] {
						ok := db.updateIndex(txnRec.Record.Key, txnRec.Record.Type, txnRec.Pos)
						if !ok {
							//return ErrIndexUpdateFailed
							panic(ErrIndexUpdateFailed)
						}

					}
					delete(txns, seqNum)
				} else {
					//事务未结束
					rec.Key = key
					txns[seqNum] = append(txns[seqNum], &data.TransactionRecord{
						Record: rec,
						Pos:    logRecPos,
					})
				}
			}
			//更新事务序列号
			curSeqNum = max(curSeqNum, seqNum)
			//更新 offset，继续读取下一个记录
			offset += size
		}
		//如果是当前活跃文件，更新这个文件的 Write offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	//更新序列号到 db 中
	db.seqNum = curSeqNum
	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory,file lock: %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.Close(); err != nil {
		return err
	}
	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNumFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(""),
		Value: []byte(strconv.FormatUint(db.seqNum, 10)),
	}
	encRec, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRec); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//关闭所有数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	err = db.activeFile.Sync()
	if err != nil {
		return err
	}
	return db.activeFile.Close()
}

func (db *DB) Size() int64 {
	return db.index.Size()
}

func (db *DB) updateIndex(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) (ok bool) {
	var oldPos *data.LogRecordPos
	switch typ {
	case data.LogRecordNormal:
		oldPos = db.index.Put(key, pos)
		ok = true
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	case data.LogRecordDeleted:
		db.reclaimSize += int64(pos.Size)
		oldPos, ok = db.index.Delete(key)
		db.reclaimSize += int64(oldPos.Size)
	default:
		panic("unknown rec type")
	}
	return
}

func (db *DB) loadIndexFromHintFile() error {
	// 查看是否存在 Hint 文件
	hintFile := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFile); os.IsNotExist(err) {
		return nil
	}
	// 打开 Hint 文件
	hintDataFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	defer hintDataFile.Close()
	// 读取 Hint 文件中的记录
	var offset int64
	for {
		rec, size, err := hintDataFile.ReadLogRecordWithSize(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		db.index.Put(rec.Key, data.DecodeLogRecordPos(rec.Value))
		offset += size
	}
	return nil
}

func (db *DB) loadSeqNum() error {
	filename := filepath.Join(db.options.DirPath, data.SeqNumFileName)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}

	seqNumFile, err := data.OpenSeqNumFile(db.options.DirPath)
	if err != nil {
		return err
	}
	defer seqNumFile.Close()
	var offset int64
	var seqNum uint64
	for {
		// 读取序列号
		rec, size, err := seqNumFile.ReadLogRecordWithSize(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		seqNum, err = strconv.ParseUint(string(rec.Value), 10, 64)
		if err != nil {
			return err
		}
		offset += size
	}
	db.seqNum = seqNum
	db.seqNumFileExists = true
	return os.Remove(filename)
}

// 将启动时的 mmap 读取文件，转换为标准 IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	err := db.activeFile.SetIOManager(fio.StandardFIO)
	if err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		err := dataFile.SetIOManager(fio.StandardFIO)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) reachMergeCondition() bool {
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return false
	}
	if db.options.DataFileMergeRatio < float32(db.reclaimSize)/float32(totalSize) {
		return true
	}
	return false
}

func (db *DB) Backup(destDir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, destDir, []string{fileLockName})
}
