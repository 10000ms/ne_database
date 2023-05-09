package base

import (
	"reflect"
	"testing"
)

func TestByteListToInt64(t *testing.T) {
	// 正常值测试用例
	data := []byte{0x00, 0x0A, 0xA1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	expectedVal := int64(437934045951)
	val, err := ByteListToInt64(data)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val != expectedVal {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal, val)
	}

	// 长度不足测试用例
	data = []byte{0x00, 0x0A, 0xA1, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = ByteListToInt64(data)
	if err == nil {
		t.Error("ByteListToInt64 failed, expected an error when len(data) < 8")
	}

	// 长度超出测试用例
	data = []byte{0x00, 0x0A, 0xA1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00}
	_, err = ByteListToInt64(data)
	if err == nil {
		t.Error("ByteListToInt64 failed, expected an error when len(data) > 8")
	}

	// 边界值测试用例
	data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}
	expectedVal = int64(9223372036854775807)
	val, err = ByteListToInt64(data)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val != expectedVal {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal, val)
	}

	data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}
	expectedVal = int64(-9223372036854775808)
	val, err = ByteListToInt64(data)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val != expectedVal {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal, val)
	}
}

func TestInt64ToByteList(t *testing.T) {
	// 正常值测试用例
	expectedData := []byte{0x00, 0x0A, 0xA1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	val := int64(437934045951)
	data, err := Int64ToByteList(val)
	if err != nil {
		t.Errorf("Int64ToByteList failed: %v", err)
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Int64ToByteList failed, expected %#v but got %#v", expectedData, data)
	}

	// 边界值测试用例
	expectedData = []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	val = int64(9223372036854775807)
	data, err = Int64ToByteList(val)
	if err != nil {
		t.Errorf("Int64ToByteList failed: %v", err)
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Int64ToByteList failed, expected %#v but got %#v", expectedData, data)
	}

	expectedData = []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	val = int64(-9223372036854775808)
	data, err = Int64ToByteList(val)
	if err != nil {
		t.Errorf("Int64ToByteList failed: %v", err)
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Int64ToByteList failed, expected %#v but got %#v", expectedData, data)
	}
}
