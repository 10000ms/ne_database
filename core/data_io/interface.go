package data_io

import (
	"ne_database/core/base"
)

type IOManager interface {
	GetTableName() string
	GetPageSize() int
	Reader(offset int64) ([]byte, base.StandardError)
	Writer(offset int64, data []byte) (bool, base.StandardError)
	Delete(offset int64) (bool, base.StandardError)
	Close() base.StandardError
	AssignEmptyPage() (int64, base.StandardError)
}

type DataManagerFunc func(initData map[int64][]byte, pageSize int) (IOManager, base.StandardError)
