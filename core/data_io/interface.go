package data_io

import (
	"ne_database/core/base"
)

type IOManager interface {
	GetTableName() string
	Reader(offset int64) ([]byte, base.StandardError)
	Writer(offset int64, data []byte) (bool, base.StandardError)
	Delete(offset int64) (bool, base.StandardError)
	Close() base.StandardError
	AssignEmptyPage() (int64, base.StandardError)
}
