package data_io

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"ne_database/core/base"
	"ne_database/utils"
)

type FileManager struct {
	tableName string
	baseDir   string
	file      *os.File
	pageSize  int
}

func InitFileManagerData(initData map[int64][]byte, pageSize int) (IOManager, base.StandardError) {
	if pageSize <= 0 {
		utils.LogError(fmt.Sprintf("[InitFileManagerData] pageSize小于等于0: %d", pageSize))
		return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("pageSize小于等于0: %d", pageSize))
	}

	tableName := fmt.Sprintf("test_data_%d_%d", rand.Intn(10000), time.Now().Unix())
	c := FileManager{
		tableName: tableName,
		baseDir:   "./",
		pageSize:  pageSize,
	}

	err := c.CreateFile(c.getTableDataFileAddr())
	if err != nil {
		utils.LogError(fmt.Sprintf("[InitFileManagerData] 创建文件失败: %s", err.Error()))
		return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}

	for offset, d := range initData {
		if d == nil {
			continue
		}
		_, err := c.Writer(offset, d)
		if err != nil {
			utils.LogError(fmt.Sprintf("[InitFileManagerData] 写入文件失败: %s", err.Error()))
			return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}

	return &c, nil
}

func (c *FileManager) GetPageSize() int {
	return c.pageSize
}

func (c *FileManager) getTableDataFileAddr() string {
	return c.baseDir + c.tableName + "." + base.DataIOFileTableDataSuffix
}

func (c *FileManager) getTableDataSchemaAddr() string {
	return c.baseDir + c.tableName + "." + base.DataIOFileTableSchemaSuffix
}

func (c *FileManager) GetTableName() string {
	return c.tableName
}

func (c *FileManager) open(fileAddr string) base.StandardError {
	_, err := os.Stat(fileAddr)
	if err != nil {
		if os.IsNotExist(err) {
			utils.LogError(fmt.Sprintf("[FileManager.open] 文件不存在: %s", err.Error()))
		}

		if os.IsPermission(err) {
			utils.LogError(fmt.Sprintf("[FileManager.open] 没有权限对文件进行操作: %s", err.Error()))
		}
		return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	f, err := os.OpenFile(fileAddr, os.O_RDWR, os.ModePerm)
	if err != nil {
		return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	c.file = f
	return nil
}

func (c *FileManager) CreateFile(addr string) base.StandardError {
	f, err := os.Create(addr)
	if err != nil {
		return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	c.file = f
	return nil
}

func (c *FileManager) Reader(offset int64) ([]byte, base.StandardError) {
	if c.file == nil {
		err := c.open(c.getTableDataFileAddr())
		if err != nil {
			return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	var (
		data = make([]byte, c.pageSize)
	)

	_, err := c.file.ReadAt(data, offset)
	if err != nil {
		return nil, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}

	return data, nil
}

func (c *FileManager) Writer(offset int64, data []byte) (bool, base.StandardError) {
	if c.file == nil {
		err := c.open(c.getTableDataFileAddr())
		if err != nil {
			return false, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	if len(data) != c.pageSize {
		return false, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf("需要写入的data长度: %d 和配置的长度: %d 不一致", len(data), c.pageSize))
	}
	_, err := c.file.WriteAt(data, offset)
	if err != nil {
		return false, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	return true, nil
}

func (c *FileManager) Delete(offset int64) (bool, base.StandardError) {
	if c.file == nil {
		err := c.open(c.getTableDataFileAddr())
		if err != nil {
			return false, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	var (
		data = make([]byte, c.pageSize)
	)
	return c.Writer(offset, data)
}

func (c *FileManager) Close() base.StandardError {
	if c.file != nil {
		err := c.file.Close()
		if err != nil {
			return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
		}
	}
	return nil
}

func (c *FileManager) AssignEmptyPage() (int64, base.StandardError) {
	if c.file == nil {
		er := c.open(c.getTableDataFileAddr())
		if er != nil {
			return 0, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, er)
		}
	}
	fi, er := os.Stat(c.getTableDataFileAddr())
	if er != nil {
		return 0, base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, er)
	}
	offset := fi.Size()
	var (
		data = make([]byte, c.pageSize)
	)
	_, err := c.Writer(offset, data)
	if err != nil {
		return 0, err
	}
	return offset, nil
}
