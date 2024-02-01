package data_io

import (
	"ne_database/core/base"
	"ne_database/core/config"
)

type MemoryManager struct {
	Storage   map[int64][]byte
	tableName string
}

func InitMemoryManagerData(initData map[int64][]byte) *MemoryManager {
	c := MemoryManager{}

	if initData != nil {
		c.Storage = initData
	} else {
		c.Storage = make(map[int64][]byte)
	}

	return &c
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
		pageSize      = config.CoreConfig.PageSize
		getNextOffset = func(times int, intervals int) int64 {
			return int64(times * intervals)
		}
	)
	for {
		nextOffset := getNextOffset(initNum, pageSize)
		if _, ok := c.Storage[nextOffset]; !ok {
			c.Storage[nextOffset] = make([]byte, 0)
			return nextOffset, nil
		}
		initNum += 1
	}

}
