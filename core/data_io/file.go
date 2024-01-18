package data_io

import (
	"fmt"
	"os"
	"time"

	"ne_database/core/base"
	"ne_database/core/config"
	"ne_database/utils"
)

type FileManager struct {
	FileAddr string
	file     *os.File
}

func InitFileManagerData(initData map[int64][]byte) *FileManager {
	addr := fmt.Sprintf("./test_data_%d", time.Now().Unix())
	c := FileManager{
		FileAddr: addr,
	}

	err := c.CreateFile(addr)
	if err != nil {
		utils.LogError(fmt.Sprintf("[InitFileManagerData] 创建文件失败: %s", err.Error()))
		return nil
	}

	for offset, d := range initData {
		if d == nil {
			continue
		}
		_, err := c.Writer(offset, d)
		if err != nil {
			utils.LogError(fmt.Sprintf("[InitFileManagerData] 写入文件失败: %s", err.Error()))
			return nil
		}
	}

	return &c
}

func (c *FileManager) open() base.StandardError {
	_, err := os.Stat(c.FileAddr)
	if err != nil {
		if os.IsNotExist(err) {
			utils.LogError(fmt.Sprintf("[FileManager.open] 文件不存在: %s", err.Error()))
		}

		if os.IsPermission(err) {
			utils.LogError(fmt.Sprintf("[FileManager.open] 没有权限对文件进行操作: %s", err.Error()))
		}
		return base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	f, err := os.OpenFile(c.FileAddr, os.O_RDWR, os.ModePerm)
	if err != nil {
		return base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	c.file = f
	return nil
}

func (c *FileManager) CreateFile(addr string) base.StandardError {
	f, err := os.Create(addr)
	if err != nil {
		return base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	c.file = f
	return nil
}

func (c *FileManager) Reader(offset int64) ([]byte, base.StandardError) {
	if c.file == nil {
		err := c.open()
		if err != nil {
			return nil, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	var (
		pageSize = config.CoreConfig.PageSize
		data     = make([]byte, pageSize)
	)

	_, err := c.file.ReadAt(data, offset)
	if err != nil {
		return nil, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}

	return data, nil
}

func (c *FileManager) Writer(offset int64, data []byte) (bool, base.StandardError) {
	if c.file == nil {
		err := c.open()
		if err != nil {
			return false, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	var (
		pageSize = config.CoreConfig.PageSize
	)
	if len(data) != pageSize {
		return false, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf("需要写入的data长度: %d 和配置的长度: %d 不一致", len(data), pageSize))
	}
	_, err := c.file.WriteAt(data, offset)
	if err != nil {
		return false, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	return true, nil
}

func (c *FileManager) Delete(offset int64) (bool, base.StandardError) {
	if c.file == nil {
		err := c.open()
		if err != nil {
			return false, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	var (
		pageSize = config.CoreConfig.PageSize
		data     = make([]byte, pageSize)
	)
	return c.Writer(offset, data)
}

func (c *FileManager) Close() base.StandardError {
	if c.file != nil {
		err := c.file.Close()
		if err != nil {
			return base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	return nil
}

func (c *FileManager) AssignEmptyPage() (int64, base.StandardError) {
	if c.file == nil {
		err := c.open()
		if err != nil {
			return 0, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	fi, err := os.Stat(c.FileAddr)
	if err != nil {
		return 0, base.NewDBError(base.FunctionModelCoreResource, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	return fi.Size(), nil
}
