package tableSchema

import (
	"ne_database/core/base"
	"testing"
)

func TestFieldInfoVerification(t *testing.T) {
	// 测试用例1：有效的 int64 类型长度
	info := &FieldInfo{
		Name:      "test_field",
		Length:    8,
		FieldType: Int64Type,
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
	if typeTest != Int64Type {
		t.Errorf("[RawToFieldType] test fail, expect: %v, got: %v", Int64Type, typeTest)
	}

	typeTest, err = RawToFieldType("string")
	if err != nil {
		t.Errorf("[RawToFieldType] The test case did not pass, expecting no errors but received an error: %s", err.Error())
	}
	if typeTest != StringType {
		t.Errorf("[RawToFieldType] test fail, expect: %v, got: %v", StringType, typeTest)
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
	metaJson := `{"name":"users","primary_key":{"name":"id","type":"int64","length":8},"value":[{"name":"name","type":"string","length":50},{"name":"age","type":"int64","length":8}]}`
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
	metaJson = `{"name":"users","primary_key":{"name":"id","type":"unknown_type","length":8},"value":[{"name":"name","type":"string","length":8},{"name":"age","type":"int64","length":8}]}`
	_, err = InitTableMetaInfoByJson(metaJson)
	if err == nil {
		t.Errorf("InitTableMetaInfoByJson should return error, but got nil")
	}
	expErr = base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("InitTableMetaInfoByJson should return error %s, but got %s", expErr.GetErrorCode(), err.GetErrorCode())
	}

	// 测试无法转换值字段数据类型的情况下是否会返回错误
	metaJson = `{"name":"users","primary_key":{"name":"id","type":"int64","length":8},"value":[{"name":"name","type":"string","length":20},{"name":"age","type":"unknown_type","length":8}]}`
	_, err = InitTableMetaInfoByJson(metaJson)
	if err == nil {
		t.Errorf("InitTableMetaInfoByJson should return error, but got nil")
	}
	expErr = base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("InitTableMetaInfoByJson should return error %s, but got %s", expErr.GetErrorCode(), err.GetErrorCode())
	}
}
