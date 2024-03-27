package core

import (
	"fmt"
	"os"
	"strings"

	"ne_database/core/base"
	"ne_database/core/config"
	"ne_database/core/tableschema"
	"ne_database/utils"
	"ne_database/utils/set"
)

type Engine struct {
}

// Init 初始化方法
func (e *Engine) Init() base.StandardError {
	return nil
}

func getTableSchemaFilePath(tableName string) string {
	return fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableName, base.DataIOFileTableSchemaSuffix)
}

func getTableDataFilePath(tableName string) string {
	return fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableName, base.DataIOFileTableDataSuffix)
}

func (e *Engine) CheckTableExist(tableName string) (bool, base.StandardError) {
	tableSchemaFilePath := getTableSchemaFilePath(tableName)
	tableDataFilePath := getTableDataFilePath(tableName)

	tableSchemaFileExist, er := utils.FileExist(tableSchemaFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("检查tableSchema文件是否存在报错: %s", er.Error())
		utils.LogError("[Engine CheckTableExist] " + errMsg)
		return false, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	tableDataFileExist, er := utils.FileExist(tableDataFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("检查tableData文件是否存在报错: %s", er.Error())
		utils.LogError("[Engine CheckTableExist] " + errMsg)
		return false, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	if tableSchemaFileExist && !tableDataFileExist {
		errMsg := fmt.Sprintf("检查tableData文件是否存在错误, schema文件存在，但data文件不存在")
		utils.LogError("[Engine CheckTableExist] " + errMsg)
		return false, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	} else if !tableSchemaFileExist && tableDataFileExist {
		errMsg := fmt.Sprintf("检查tableData文件是否存在错误, schema文件不存在，但data文件存在")
		utils.LogError("[Engine CheckTableExist] " + errMsg)
		return false, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	} else if !tableSchemaFileExist && !tableDataFileExist {
		return false, nil
	} else {
		return true, nil
	}
}

func (e *Engine) LoadTableSchemaInfo(tableName string) (*tableschema.TableMetaInfo, base.StandardError) {
	tableSchemaFilePath := getTableSchemaFilePath(tableName)

	bytes, er := os.ReadFile(tableSchemaFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("读取文件时发生错误: %s", er.Error())
		utils.LogError("[Engine LoadTableSchemaInfo] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	tableSchema, err := tableschema.InitTableMetaInfoByJson(string(bytes))
	if err != nil {
		return nil, err
	}
	return tableSchema, nil
}

func (e *Engine) DeleteTable(tableName string) base.StandardError {
	exist, err := e.CheckTableExist(tableName)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreEngine), 1)(fmt.Sprintf("[DeleteTable] CheckTableExist错误, %s", err.Error()))
		return err
	}
	if !exist {
		utils.LogDev(string(base.FunctionModelCoreEngine), 10)(fmt.Sprintf("[Engine.DeleteTable] 表 %s 不存在，所以无需删除", err.Error()))
		return nil
	}

	tableSchemaFilePath := getTableSchemaFilePath(tableName)
	tableDataFilePath := getTableDataFilePath(tableName)

	er := os.Remove(tableSchemaFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("删除 %s 的TableSchema发生错误: %s", tableSchemaFilePath, er.Error())
		utils.LogError("[Engine DeleteTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	er = os.Remove(tableDataFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("删除 %s 的TableData发生错误: %s", tableDataFilePath, er.Error())
		utils.LogError("[Engine DeleteTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	return nil
}

func (e *Engine) AllTable() (map[string]*tableschema.TableMetaInfo, base.StandardError) {

	entries, err := os.ReadDir(config.CoreConfig.FileAddr)
	if err != nil {
		errMsg := fmt.Sprintf("读取 %s 目录发生错误: %s", config.CoreConfig.FileAddr, err.Error())
		utils.LogError("[Engine AllTable] " + errMsg)
		return nil, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	var (
		tableSet       = set.NewStringsSet()
		allTableSchema = make(map[string]*tableschema.TableMetaInfo)
	)

	for _, entry := range entries {
		if entry != nil {
			info, err := entry.Info()
			if err != nil {
				errMsg := fmt.Sprintf("读取 %s 文件info发生错误: %s", config.CoreConfig.FileAddr, err.Error())
				utils.LogError("[Engine AllTable] " + errMsg)
				return nil, base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeIO, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
			}
			if !info.IsDir() {
				// 不是文件夹，且是目标文件
				rawName := info.Name()
				if strings.HasSuffix(info.Name(), base.DataIOFileTableSchemaSuffix) {
					n := strings.Replace(rawName, fmt.Sprintf(".%s", base.DataIOFileTableSchemaSuffix), "", 1)
					tableSet.Add(n)
				} else if strings.HasSuffix(info.Name(), base.DataIOFileTableDataSuffix) {
					n := strings.Replace(rawName, fmt.Sprintf(".%s", base.DataIOFileTableDataSuffix), "", 1)
					tableSet.Add(n)
				}

			}
		}
	}
	for _, n := range tableSet.TotalMember() {
		if n == "" {
			continue
		}
		schema, er := e.LoadTableSchemaInfo(n)
		if er != nil {
			return nil, er
		}
		allTableSchema[schema.Name] = schema
	}
	return allTableSchema, nil
}

// CreateTable 建表
func (e *Engine) CreateTable(tableInfo *tableschema.TableMetaInfo) base.StandardError {
	var (
		err base.StandardError
		er  error
	)

	if tableInfo == nil {
		errMsg := "输入的tableInfo为空"
		utils.LogError("[Engine CreateTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeTableSchemaError, fmt.Errorf(errMsg))
	}
	err = tableInfo.Verification()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreEngine), 1)(fmt.Sprintf("[CreateTable] 表校验错误, %s", err.Error()))
		return err
	}

	tableSchemaFilePath := getTableSchemaFilePath(tableInfo.Name)
	tableDataFilePath := getTableDataFilePath(tableInfo.Name)

	exist, err := e.CheckTableExist(tableInfo.Name)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreEngine), 1)(fmt.Sprintf("[CreateTable] CheckTableExist错误, %s", err.Error()))
		return err
	}
	if exist {
		errMsg := fmt.Sprintf("表: %s 文件已存在", tableInfo.Name)
		utils.LogError("[Engine CreateTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}

	tableInfoByte, err := tableInfo.TableMetaInfoToJsonByte()
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreEngine), 1)(fmt.Sprintf("[CreateTable] TableMetaInfoToJsonStr错误, %s", err.Error()))
		return err
	}

	tableSchemaFile, er := os.Create(tableSchemaFilePath)
	if er != nil {
		utils.LogError("[Engine CreateTable] 建表Schema错误" + er.Error())
		return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}
	_, er = tableSchemaFile.Write(tableInfoByte)

	_, er = os.Create(tableDataFilePath)
	if er != nil {
		utils.LogError("[Engine CreateTable] 建表Data错误" + er.Error())
		return base.NewDBError(base.FunctionModelCoreDataIO, base.ErrorTypeIO, base.ErrorBaseCodeIOError, err)
	}

	return nil
}

// Select 表查询
//func (e *Engine) Select(tableName string, whereArgs []*base.WherePartItem) (int64, [][]byte, base.StandardError) {
// // TODO
//}

//// Insert 表插入
//func (e *Engine) Insert() (int64, base.StandardError) {
//
//}
//
//// Update 表更新
//func (e *Engine) Update() (int64, base.StandardError) {
//
//}
//
//// Delete 表删除
//func (e *Engine) Delete() (int64, base.StandardError) {
//
//}
