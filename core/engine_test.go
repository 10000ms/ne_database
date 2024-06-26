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

func TestEngine_CheckTableExist(t *testing.T) {
	testName := "testTable"
	tableSchemaFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, testName, base.DataIOFileTableSchemaSuffix)
	tableDataFilePath := fmt.Sprintf("%s%s.%s", config.CoreConfig.FileAddr, testName, base.DataIOFileTableDataSuffix)

	e := Engine{}
	exist, err := e.CheckTableExist(testName)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if exist {
		t.Errorf("unexpected value: %v", exist)
		return
	}

	_, er := os.Create(tableSchemaFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}

	exist, err = e.CheckTableExist(testName)
	if err == nil {
		t.Errorf("expected error, but got: %v", err)
		return
	}

	_, er = os.Create(tableDataFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}

	exist, err = e.CheckTableExist(testName)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !exist {
		t.Errorf("unexpected value: %v", exist)
		return
	}

	er = os.Remove(tableSchemaFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}

	exist, err = e.CheckTableExist(testName)
	if err == nil {
		t.Errorf("expected error, but got: %v", err)
		return
	}

	er = os.Remove(tableDataFilePath)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}

	exist, err = e.CheckTableExist(testName)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if exist {
		t.Errorf("unexpected value: %v", exist)
		return
	}

}

func TestEngine_LoadTableSchemaInfo(t *testing.T) {
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    8,
				FieldType: tableschema.BigIntType,
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

	loadTable, err := e.LoadTableSchemaInfo(tableInfo.Name)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	isSame := loadTable.CompareTableInfo(tableInfo)
	if !isSame {
		t.Errorf("unexpected same")
		return
	}

	// 测试之后删除
	er := e.DeleteTable(tableInfo.Name)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
}

func TestEngine_CreateTable(t *testing.T) {
	tableInfo := &tableschema.TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    8,
				FieldType: tableschema.BigIntType,
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
	er = e.DeleteTable(tableInfo.Name)
	if er != nil {
		t.Errorf("unexpected error: %v", er)
		return
	}
}

func TestEngine_AllTable(t *testing.T) {
	e := Engine{}

	userTableName := "users"
	userTableInfo := &tableschema.TableMetaInfo{
		Name: userTableName,
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "age",
				Length:    8,
				FieldType: tableschema.BigIntType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	err := e.CreateTable(userTableInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	personTableName := "person"
	personTableInfo := &tableschema.TableMetaInfo{
		Name: personTableName,
		PrimaryKeyFieldInfo: &tableschema.FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: tableschema.BigIntType,
		},
		ValueFieldInfo: []*tableschema.FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: tableschema.CharType,
			},
			{
				Name:      "idCard",
				Length:    8,
				FieldType: tableschema.BigIntType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	err = e.CreateTable(personTableInfo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	allTableSchema, err := e.AllTable()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if len(allTableSchema) != 2 {
		t.Errorf("unexpected num")
		return
	}

	isSame := allTableSchema[userTableName].CompareTableInfo(userTableInfo)
	if !isSame {
		t.Errorf("expected same")
		return
	}
	isSame = allTableSchema[personTableName].CompareTableInfo(personTableInfo)
	if !isSame {
		t.Errorf("expected same")
		return
	}

	// 测试之后删除
	err = e.DeleteTable(userTableName)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	err = e.DeleteTable(personTableName)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
}
