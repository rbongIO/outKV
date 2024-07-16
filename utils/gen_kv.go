package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
)

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key_{%d}", i))
}

// GetTestValue 生成随机长度的字符串作为 value，用于测试
func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcast-go-value" + string(b))
}

func GenKV(n int) (keys, values [][]byte) {
	for i := 0; i < n; i++ {
		keys = append(keys, GetTestKey(i))
		values = append(values, GetTestValue(10))
	}
	return
}
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcast-go-value" + string(b))
}
