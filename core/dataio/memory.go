package dataio

import (
	"fmt"

	"ne_database/core/base"
	"ne_database/utils"
)

type MemoryManager struct {
	Storage   map[int64][]byte
	tableName string
	pageSize  int
}

func InitMemoryManagerData(initData map[int64][]byte, pageSize int) (IOManager, base.StandardError) {
	if pageSize <= 0 {
		utils.LogError(fmt.Sprintf("[InitMemoryManagerData] pageSize小于等于0: %d", pageSize))
		return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("pageSize小于等于0: %d", pageSize))
	}

	c := MemoryManager{
		pageSize: pageSize,
	}

	if initData != nil {
		c.Storage = initData
	} else {
		c.Storage = make(map[int64][]byte)
	}

	return &c, nil
}

func (c *MemoryManager) GetPageSize() int {
	return c.pageSize
}

func (c *MemoryManager) GetTableName() string {
	return c.tableName
}

func (c *MemoryManager) Reader(offset int64) ([]byte, base.StandardError) {
	if page, ok := c.Storage[offset]; ok {
		return page, nil
	}
	return make([]byte, 0), nil
}

func (c *MemoryManager) Writer(offset int64, data []byte) (bool, base.StandardError) {
	if len(data) != c.pageSize {
		return false, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf("需要写入的data长度: %d 和配置的长度: %d 不一致", len(data), c.pageSize))
	}
	c.Storage[offset] = data
	return true, nil
}

func (c *MemoryManager) Delete(offset int64) (bool, base.StandardError) {
	delete(c.Storage, offset)
	return true, nil
}

func (c *MemoryManager) Close() base.StandardError {
	return nil
}

func (c *MemoryManager) AssignEmptyPage() (int64, base.StandardError) {
	var (
		initNum       = 1
		getNextOffset = func(times int, intervals int) int64 {
			return int64(times * intervals)
		}
	)
	for {
		nextOffset := getNextOffset(initNum, c.pageSize)
		if _, ok := c.Storage[nextOffset]; !ok {
			c.Storage[nextOffset] = make([]byte, 0)
			return nextOffset, nil
		}
		initNum += 1
	}

}
