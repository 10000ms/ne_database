package core

import (
	"testing"

	tableSchema "ne_database/core/table_schema"
)

func TestGetNoLeafNodeByteDataReadLoopData(t *testing.T) {
	// 构造测试数据
	data := []byte{0, 0, 0, 1, 'a', 'b', 'c'}
	loopTime := 0 // 测试第一轮解析
	primaryKeyInfo := &tableSchema.FieldInfo{
		Name:      "id",
		Length:    3,
		FieldType: tableSchema.Int64Type,
	}

	// 调用被测试函数
	result, err := getNoLeafNodeByteDataReadLoopData(data, loopTime, primaryKeyInfo)

	// 断言结果
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result.Offset != 1 {
		t.Errorf("expected offset to be 1, but got %d", result.Offset)
	}
	if !result.OffsetSuccess {
		t.Errorf("expected offsetSuccess to be true, but got false")
	}
	if !result.PrimaryKeySuccess {
		t.Errorf("expected primaryKeySuccess to be true, but got false")
	}
	if string(result.PrimaryKey.Value) != "abc" {
		t.Errorf("expected primaryKey value to be 'abc', but got %q", result.PrimaryKey.Value)
	}
	if *result.PrimaryKey.Type != tableSchema.Int64Type {
		t.Errorf("expected primaryKey type to be EnumTypeString, but got %v", result.PrimaryKey.Type.Type())
	}
}
