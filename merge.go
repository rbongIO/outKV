package bitcask_go

import (
	"github.com/rbongIO/bitcask-go/data"
	"github.com/rbongIO/bitcask-go/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeFinishedKey = "MergeFinished"
	mergeDirName     = "-merge"
)

func (db *DB) Merge() error {
	//如果数据库为空责直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	//如果正在合并数据文件，则直接返回
	if db.isMerging {
		return ErrMergeIsProcessing
	}
	//检查是否达到 merge 条件
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return err
	}
	if db.options.DataFileMergeRatio > float32(db.reclaimSize)/float32(totalSize) {
		return ErrMergeRatioUnreached
	}
	// 价差剩余空间容量是否容纳产生的 merge 文件
	availableDiskSpace, err := utils.AvailableSpace()
	if err != nil {
		return err
	}
	if availableDiskSpace <= uint64(totalSize-db.reclaimSize) {
		return ErrDiskSpaceNotEnough
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.Sync(); err != nil {
		return err
	}

	// 将当前活跃文件保存为旧文件
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	// 创建新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		return err
	}
	nonMergeFileID := db.activeFile.FileID

	// 取出所有等待 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	// 对文件进行排序，从小到大
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过 merge，将其删掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 创建 merge 目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时 bitcask 实例
	mergeDB, err := Open(WithDirPath(mergePath), WithIndexType(db.options.IndexType),
		WithMaxDataFileSize(db.options.MaxDataFileSize), WithSyncWrite(false))
	if err != nil {
		return err
	}
	defer mergeDB.Close()
	//打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 将所有等待 merge 的文件添加到 mergeDB 中，进行重写
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			rec, size, err := dataFile.ReadLogRecordWithSize(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//解析到实际的 key
			key, _ := parseLogRecordKey(rec.Key)
			curPos := db.index.Get(key)
			//和内存中的索引位置进行比较，如果有效则重写
			if curPos != nil && curPos.Fid == dataFile.FileID && curPos.Offset == offset {
				// 清楚事务标记
				rec.Key = logRecordKeyWithSeqNum(key, nonTransactionSeqNum)
				pos, err := mergeDB.appendLogRecord(rec)
				if err != nil {
					return err
				}
				// 写入位置索引到 hint 文件
				if err := hintFile.WriteHintRecord(key, pos); err != nil {
					return err
				}
			}
			//读取吓一跳记录
			offset += size
		}
	}
	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	hintFile.Close()
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	// 标识 merge 完成
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRec := &data.LogRecord{Key: []byte(mergeFinishedKey), Value: []byte(strconv.Itoa(int(nonMergeFileID)))}
	encMergeRec, _ := data.EncodeLogRecord(mergeFinRec)
	if err := mergeFinFile.Write(encMergeRec); err != nil {
		return err
	}
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}
	return nil
}

// /tmp/bitcask
// /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找标识 merge 完成的文件，判断 merge 是否有效
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedName {
			mergeFinished = true
			continue
		}
		if entry.Name() == data.SeqNumFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	// 如果没有标识文件，说明 merge 没有完成，直接返回
	if !mergeFinished {
		return nil
	}
	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}
	// 删除就得数据文件
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(mergePath, fileID)
		if _, err := os.Stat(fileName); err == nil {
			err := os.Remove(fileName)
			if err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动过来
	for _, fileName := range mergeFileNames {
		// /tmp/bitcask-merge 00.data 11.data
		// /tmp/bitcask 00.data 11.data
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	defer mergeFinishedFile.Close()
	rec, _, err := mergeFinishedFile.ReadLogRecordWithSize(0)
	if err != nil {
		return 0, err
	}
	fileID, err := strconv.Atoi(string(rec.Value))
	if err != nil {
		return 0, err
	}
	return uint32(fileID), nil
}
