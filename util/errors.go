package util

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("The key is empty.")
	ErrIndexUpdateFailed      = errors.New("Failed to update the index.")
	ErrKeyNotFound            = errors.New("The key was not found in the index.")
	ErrDataFileNotFound       = errors.New("The data file was not found.")
	ErrDataDirEmpty           = errors.New("database dir path is empty.")
	ErrDataFileMaxSizeInvalid = errors.New("data file max size must greater than zero.")
	ErrDataDirCorrupted       = errors.New("The database directory maybe corrupted.")
	ErrDataDeleteFailed       = errors.New("Delete data log record failed.")
	ErrInvalidCRC             = errors.New("Invalid crc value, log record maybe corrupted.")
	ErrExceedMaxBatchNum      = errors.New("Exceed the max batch num.")
	ErrMergeisInProgress      = errors.New("The merge is in progress, try again later.")
	ErrDatabaseIsUsing        = errors.New("The database directory is used by another progress")
	ErrDataMergeRatioInvlid   = errors.New("Invlid merge ratio, must between 0 and 1.")
	ErrMergeRatioUnreached    = errors.New("The merge ratio do not reach the ratio")
	ErrNoEnoughSpaceForMerge  = errors.New("No enough disk space for merge operation")
)
