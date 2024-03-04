package core

import (
	"fmt"
	"os"

	"ne_database/core/base"
	"ne_database/core/config"
	"ne_database/core/tableschema"
	"ne_database/utils"
)

type Engine struct {
}

// Init 初始化方法
func (e *Engine) Init() base.StandardError {
	return nil
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

	tableSchemaFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableInfo.Name, base.DataIOFileTableSchemaSuffix)
	tableDataFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableInfo.Name, base.DataIOFileTableDataSuffix)

	// 检查表是否已经存在
	tableSchemaFileExist, er := utils.FileExist(tableSchemaFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("检查tableSchema文件是否存在报错: %s", er.Error())
		utils.LogError("[Engine CreateTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	if tableSchemaFileExist {
		errMsg := fmt.Sprintf("表: %s 的tableSchema文件已存在", tableInfo.Name)
		utils.LogError("[Engine CreateTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	tableDataFileExist, er := utils.FileExist(tableDataFilePath)
	if er != nil {
		errMsg := fmt.Sprintf("检查tableData文件是否存在报错: %s", er.Error())
		utils.LogError("[Engine CreateTable] " + errMsg)
		return base.NewDBError(base.FunctionModelCoreEngine, base.ErrorTypeInput, base.ErrorBaseCodeIOError, fmt.Errorf(errMsg))
	}
	if tableDataFileExist {
		errMsg := fmt.Sprintf("表: %s 的tableData文件已存在", tableInfo.Name)
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
