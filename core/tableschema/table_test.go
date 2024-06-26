package tableschema

import (
	"os"
	"testing"

	"ne_database/core/base"
	"ne_database/core/config"
)

func TestFieldInfo_Verification(t *testing.T) {
	// 测试用例1：有效的 int64 类型长度
	info := &FieldInfo{
		Name:      "test_field",
		Length:    8,
		FieldType: BigIntType,
	}

	err := info.Verification()

	if err != nil {
		t.Errorf("TestFieldInfo_Verification() 测试用例1未通过，期望结果是没有错误，但得到了错误%v", err)
	}

	// 测试用例2：无效的 int64 类型长度
	info.Length = 4

	err = info.Verification()

	if err == nil {
		t.Error("TestFieldInfo_Verification() 测试用例2未通过，期望结果是类型长度错误，但得到了没有错误")
	} else if err.Error() != "int64类型长度错误: 4" {
		t.Errorf("TestFieldInfo_Verification() 测试用例2未通过，期望的错误信息是\"int64类型长度错误: 4\"，但得到了%s", err.Error())
	}
}

func TestRawToFieldType(t *testing.T) {
	typeTest, err := RawToFieldType("int64")
	if err != nil {
		t.Errorf("[RawToFieldType] The test case did not pass, expecting no errors but received an error: %s", err.Error())
	}
	if typeTest != BigIntType {
		t.Errorf("[RawToFieldType] test fail, expect: %v, got: %v", BigIntType, typeTest)
	}

	typeTest, err = RawToFieldType("string")
	if err != nil {
		t.Errorf("[RawToFieldType] The test case did not pass, expecting no errors but received an error: %s", err.Error())
	}
	if typeTest != CharType {
		t.Errorf("[RawToFieldType] test fail, expect: %v, got: %v", CharType, typeTest)
	}

	typeTest, err = RawToFieldType("errorType")
	expErr := base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err == nil {
		t.Error("[RawToFieldType] test fail, expected result was an error, but there was no error received")
	}
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("[RawToFieldType] expect error: %s, got err: %s", expErr, err)
	}
}

func TestInitTableMetaInfoByJson(t *testing.T) {
	// 测试正常情况下能否正确解析 json 并返回 TableMetaInfo 实例
	metaJson := `{"name":"users","primary_key":{"name":"id","type":"int64","length":8},"value":[{"name":"name","type":"string","length":50},{"name":"age","type":"int64","length":8}],"page_size":1000,"storage_type":"file"}`
	expectedPKName := "id"
	expectedValueFieldsCount := 2
	meta, err := InitTableMetaInfoByJson(metaJson)
	if err != nil {
		t.Errorf("InitTableMetaInfoByJson should not return error, but got %v", err)
	}
	if meta.Name != "users" {
		t.Errorf("Expected name to be 'users', but got %s", meta.Name)
	}
	if meta.PrimaryKeyFieldInfo.Name != expectedPKName {
		t.Errorf("Expected primary key name to be %s, but got %s", expectedPKName, meta.PrimaryKeyFieldInfo.Name)
	}
	if len(meta.ValueFieldInfo) != expectedValueFieldsCount {
		t.Errorf("Expected number of value fields to be %d, but got %d", expectedValueFieldsCount, len(meta.ValueFieldInfo))
	}

	// 测试无法解析 json 的情况下是否会返回错误
	metaJson = `{1}`
	_, err = InitTableMetaInfoByJson(metaJson)
	if err == nil {
		t.Errorf("InitTableMetaInfoByJson should return error, but got nil")
	}
	expErr := base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, nil)
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("InitTableMetaInfoByJson should return error %s, but got %s", expErr.GetErrorCode(), err.GetErrorCode())
	}

	// 测试无法转换主键数据类型的情况下是否会返回错误
	metaJson = `{"name":"users","primary_key":{"name":"id","type":"unknown_type","length":8},"value":[{"name":"name","type":"string","length":8},{"name":"age","type":"int64","length":8}],"page_size":1000,"storage_type":"file"}`
	_, err = InitTableMetaInfoByJson(metaJson)
	if err == nil {
		t.Errorf("InitTableMetaInfoByJson should return error, but got nil")
	}
	expErr = base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("InitTableMetaInfoByJson should return error %s, but got %s", expErr.GetErrorCode(), err.GetErrorCode())
	}

	// 测试无法转换值字段数据类型的情况下是否会返回错误
	metaJson = `{"name":"users","primary_key":{"name":"id","type":"int64","length":8},"value":[{"name":"name","type":"string","length":20},{"name":"age","type":"unknown_type","length":8}],"page_size":1000,"storage_type":"file"}`
	_, err = InitTableMetaInfoByJson(metaJson)
	if err == nil {
		t.Errorf("InitTableMetaInfoByJson should return error, but got nil")
	}
	expErr = base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("InitTableMetaInfoByJson should return error %s, but got %s", expErr.GetErrorCode(), err.GetErrorCode())
	}
}

func TestFieldInfo_CompareFieldInfo(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	info := &FieldInfo{
		Name:         "test_field",
		Length:       8,
		FieldType:    BigIntType,
		DefaultValue: "1",
	}

	info2 := &FieldInfo{
		Name:         "test_field",
		Length:       8,
		FieldType:    BigIntType,
		DefaultValue: "1",
	}

	isSame := info.CompareFieldInfo(info2)
	if !isSame {
		t.Error("CompareFieldInfo Expected true, but got false")
		return
	}

	info3 := &FieldInfo{
		Name:         "test_field1",
		Length:       8,
		FieldType:    BigIntType,
		DefaultValue: "1",
	}

	isSame = info.CompareFieldInfo(info3)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	info4 := &FieldInfo{
		Name:         "test_field",
		Length:       7,
		FieldType:    BigIntType,
		DefaultValue: "1",
	}

	isSame = info.CompareFieldInfo(info4)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	info5 := &FieldInfo{
		Name:         "test_field",
		Length:       8,
		FieldType:    CharType,
		DefaultValue: "1",
	}

	isSame = info.CompareFieldInfo(info5)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	info6 := &FieldInfo{
		Name:         "test_field",
		Length:       8,
		FieldType:    BigIntType,
		DefaultValue: "2",
	}

	isSame = info.CompareFieldInfo(info6)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}
}

func TestTableMetaInfo_CompareTableInfo(t *testing.T) {
	_ = os.Setenv("LOG_DEV", "1")
	_ = os.Setenv("LOG_DEV_LEVEL", "0")
	_ = os.Setenv("LOG_DEV_MODULES", "All")

	tableInfo1 := &TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	tableInfo2 := &TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	isSame := tableInfo1.CompareTableInfo(tableInfo2)
	if !isSame {
		t.Error("CompareFieldInfo Expected true, but got false")
		return
	}

	tableInfo3 := &TableMetaInfo{
		Name: "users1",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	isSame = tableInfo1.CompareTableInfo(tableInfo3)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	tableInfo4 := &TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "i",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
			{
				Name:      "age",
				Length:    4 * 2, // 假设最长2字
				FieldType: CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	isSame = tableInfo1.CompareTableInfo(tableInfo4)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	tableInfo5 := &TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	isSame = tableInfo1.CompareTableInfo(tableInfo5)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}

	tableInfo6 := &TableMetaInfo{
		Name: "users",
		PrimaryKeyFieldInfo: &FieldInfo{
			Name:      "id",
			Length:    8,
			FieldType: BigIntType,
		},
		ValueFieldInfo: []*FieldInfo{
			{
				Name:      "name",
				Length:    4 * 5, // 假设最长5字
				FieldType: CharType,
			},
			{
				Name:      "age",
				Length:    8,
				FieldType: BigIntType,
			},
		},
		PageSize:    config.CoreConfig.PageSize,
		StorageType: base.StorageTypeFile,
	}

	isSame = tableInfo1.CompareTableInfo(tableInfo6)
	if isSame {
		t.Error("CompareFieldInfo Expected false, but got true")
		return
	}
}
