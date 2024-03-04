package dataio

import (
	"fmt"

	"ne_database/core/base"
)

func GetManagerInitFuncByType(t string) (DataManagerFunc, base.StandardError) {
	switch t {
	case base.StorageTypeMemory:
		return InitMemoryManagerData, nil
	case base.StorageTypeFile:
		return InitFileManagerData, nil
	default:
		return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("不支持类型: %s", t))
	}
}
