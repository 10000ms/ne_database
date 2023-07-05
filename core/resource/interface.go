package resource

import (
	"ne_database/core/base"
)

type IOManager interface {
	Reader(offset int64) ([]byte, base.StandardError)
	Writer(offset int64, data []byte) (bool, base.StandardError)
	Delete(offset int64) (bool, base.StandardError)
	AssignEmptyPage() (int64, base.StandardError)
}
