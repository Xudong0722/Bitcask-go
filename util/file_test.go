package util

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	//dir, _ := os.Getwd()
	dirSize, err := DirSize(filepath.Join("/tmp/test-dir"))
	assert.Nil(t, err)
	t.Log(dirSize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)

	t.Log(size / 1024 / 1024 / 1024) //GB
}
