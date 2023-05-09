package tableSchema

import (
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
