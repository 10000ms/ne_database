package core

import (
	"fmt"

	"ne_database/core/base"
	tableSchema "ne_database/core/table_schema"
	"ne_database/utils"
)

type Engine struct {
}

// Init 初始化方法
func (e *Engine) Init() base.StandardError {
	return nil
}

// CreateTable TODO
func (e *Engine) CreateTable(tableInfo *tableSchema.TableMetaInfo) base.StandardError {
	var err base.StandardError

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

	//tableInfoStr, err := tableInfo.TableMetaInfoToJsonStr()
	//if err != nil {
	//	utils.LogDev(string(base.FunctionModelCoreEngine), 1)(fmt.Sprintf("[CreateTable] TableMetaInfoToJsonStr错误, %s", err.Error()))
	//	return err
	//}

	return nil
}
