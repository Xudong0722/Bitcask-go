package util

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("The key is empty.")
	ErrIndexUpdateFailed = errors.New("Failed to update the index.")
	ErrKeyNotFound       = errors.New("The key was not found in the index.")
	ErrDataFileNotFound  = errors.New("The data file was not found.")
	ErrDataDirEmpty      = errors.New("database dir path is empty.")
  	ErrDataFileMaxSizeInvalid = errors.New("data file max size must greater than zero.")
	ErrDataDirCorrupted  = errors.New("The database directory maybe corrupted.")
)
