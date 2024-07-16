package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("the key is not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
	ErrBatchNumExceeded       = errors.New("batch num exceeded")
	ErrMergeIsProcessing      = errors.New("merge is processing,try again later")
	ErrDatabaseIsUsing        = errors.New("database is using")
	ErrMergeRatioUnreached    = errors.New("merge ratio unreached")
	ErrDiskSpaceNotEnough     = errors.New("disk space not enough to merge ")
)
