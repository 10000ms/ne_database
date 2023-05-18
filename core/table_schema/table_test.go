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
	expErr := base.NewDBError(base.FunctionModelCoreDTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeInnerParameterError, nil)
	if err == nil {
		t.Error("[RawToFieldType] test fail, expected result was an error, but there was no error received")
	}
	if err.GetErrorCode() != expErr.GetErrorCode() {
		t.Errorf("[RawToFieldType] expect error: %s, got err: %s", expErr, err)
	}
}
