package core

import (
	"fmt"
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/config"
	"ne_database/core/tableschema"
	"ne_database/utils"
)

func TestEngine_CreateTable(t *testing.T) {
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.Int64Type,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.StringType,
			},
			{
				Name:      "age",
				Length:    8,
				FieldType: tableschema.Int64Type,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	e := Engine{}
	err := e.CreateTable(tableInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	tableSchemaFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableInfo.Name, base.DataIOFileTableSchemaSuffix)
	tableDataFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, tableInfo.Name, base.DataIOFileTableDataSuffix)

	// 检查表文件是否存在
	tableSchemaFileExist, er := utils.FileExist(tableSchemaFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	if !tableSchemaFileExist {
		t.Errorf("tableSchemaFileExist not Exist")
		return
	}
	tableDataFileExist, er := utils.FileExist(tableDataFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	if !tableDataFileExist {
		t.Errorf("tableDataFileExist not Exist")
		return
	}

	// 检查Schema写入的内容是否正确
	tableInfoJosnByte, err := tableInfo.TableMetaInfoToJsonByte()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	tableSchemaFile, er := os.OpenFile(tableSchemaFilePath, os.O_RDWR, os.ModePerm)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	data := make([]byte, len(tableInfoJosnByte))
	_, er = tableSchemaFile.Read(data)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	testTable, err := tableschema.InitTableMetaInfoByJson(string(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	isSame := tableInfo.CompareTableInfo(testTable)
	if !isSame {
		t.Errorf("expect same")
		return
	}

	// 检查Schema文件长度
	tableSchemaFileInfo, er := tableSchemaFile.Stat()
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	if int(tableSchemaFileInfo.Size()) != len(tableInfoJosnByte) {
		t.Errorf("unexpect file length")
		return
	}

	// 检查数据文件是否为空
	tableDataFile, er := os.OpenFile(tableDataFilePath, os.O_RDWR, os.ModePerm)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	tableDataFileInfo, er := tableDataFile.Stat()
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	if tableDataFileInfo.Size() != 0 {
		t.Errorf("unexpect file length")
		return
	}

	// 测试之后删除
	er = os.Remove(tableSchemaFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
	er = os.Remove(tableDataFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
}
