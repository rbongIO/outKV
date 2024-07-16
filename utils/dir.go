package utils

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// 获取磁盘剩余空间
func AvailableSpace() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(wd, &fs)
	if err != nil {
		return 0, err
	}
	return uint64(fs.Bavail) * uint64(fs.Bsize), nil
}

func CopyDir(src, dest string, exclude []string) error {
	//判断目标目录是否存在
	if _, err := os.Stat(dest); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(dest, os.ModePerm); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		filename := strings.Replace(path, src, "", 1)
		if filename == "" {
			return nil
		}
		for _, ex := range exclude {
			matched, err := filepath.Match(ex, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			err := os.MkdirAll(filepath.Join(dest, filename), info.Mode())
			if err != nil {
				return err
			}
		}
		data, err := os.ReadFile(filepath.Join(src, filename))
		if err != nil {
			return err
		}
		err = os.WriteFile(filepath.Join(dest, filename), data, info.Mode())
		if err != nil {
			return err
		}
		return nil
	})
}
