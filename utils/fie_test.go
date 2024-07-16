package utils

import "testing"

func TestAvailableSpace(t *testing.T) {
	size, err := AvailableSpace()
	if err != nil {
		t.Errorf("AvailableSpace() error = %v", err)
		return
	}
	t.Logf("AvailableSpace() = %vGB", size/1024/1024/1024)
}
