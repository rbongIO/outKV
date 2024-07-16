package utils

import (
	"testing"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := GetTestKey(i)
		t.Logf("key: %s\n", key)
	}
}

func TestGetTestValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		value := GetTestValue(10)
		t.Logf("value: %s\n", value)
	}
}
