package base

import (
	"reflect"
	"testing"
)

func TestByteListToInt64(t *testing.T) {
	// 正整数测试用例
	data1 := []byte{0x00, 0x00, 0x00, 0xCC, 0xCD, 0x7A, 0xA7, 0x2F}
	expectedVal1 := int64(879620695855)
	val1, err := ByteListToInt64(data1)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val1 != expectedVal1 {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal1, val1)
	}

	// 正整数测试用例2
	data2 := []byte{0x00, 0x00, 0x00, 0x65, 0xF6, 0xE7, 0x3A, 0xE1}
	expectedVal2 := int64(437934045921)
	val2, err := ByteListToInt64(data2)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val2 != expectedVal2 {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal2, val2)
	}

	// 负整数测试用例
	data3 := []byte{0xFF, 0xFF, 0xFF, 0x9A, 0x09, 0x18, 0xC5, 0x01}
	expectedVal3 := int64(-437934045951)
	val3, err := ByteListToInt64(data3)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val3 != expectedVal3 {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal3, val3)
	}

	// 长度不足测试用例
	data := []byte{0x00, 0x0A, 0xA1, 0xFF, 0xFF, 0xFF, 0xFF}
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
	data = []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	expectedVal := int64(9223372036854775807)
	val, err := ByteListToInt64(data)
	if err != nil {
		t.Errorf("ByteListToInt64 failed: %v", err)
	}
	if val != expectedVal {
		t.Errorf("ByteListToInt64 failed, expected %d but got %d", expectedVal, val)
	}
	// 边界值测试用例2
	data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F}
	expectedVal = int64(-225)
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
	expectedData := []byte{0x00, 0x00, 0x00, 0xCC, 0xCD, 0x7A, 0xA7, 0x2F}
	val := int64(879620695855)
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
	// 边界值测试用例2
	expectedData = []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	val = int64(-9223372036854775808)
	data, err = Int64ToByteList(val)
	if err != nil {
		t.Errorf("Int64ToByteList failed: %v", err)
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Int64ToByteList failed, expected %#v but got %#v", expectedData, data)
	}
	// 边界值测试用例3
	expectedData = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F}
	val = int64(-225)
	data, err = Int64ToByteList(val)
	if err != nil {
		t.Errorf("Int64ToByteList failed: %v", err)
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Int64ToByteList failed, expected %#v but got %#v", expectedData, data)
	}
}

func TestByteListToString(t *testing.T) {
	data := []byte{
		0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0xe4, 0xb8, 0x96, 0xe7, 0x95, 0x8c, 0x21, 0xf0, 0x9f, 0x91, 0x8b,
		0xec, 0x95, 0x88, 0xeb, 0x85, 0x95, 0xed, 0x95, 0x98, 0xec, 0x84, 0xb8, 0xec, 0x9a, 0x94, 0xe3, 0x80, 0x82,
		0xe6, 0x97, 0xa5, 0xe6, 0x9c, 0xac, 0xe8, 0xaa, 0x9e, 0xe3, 0x81, 0xa7, 0xe6, 0x9b, 0xb8, 0xe3, 0x81, 0x84,
		0xe3, 0x81, 0xa6, 0xe3, 0x81, 0xbf, 0xe3, 0x81, 0xbe, 0xe3, 0x81, 0x99, 0xe3, 0x80, 0x82, 0x31, 0x32, 0x33,
		0xf0, 0x9f, 0x98, 0x8a, 0x0a,
	}
	expectedStr := "Hello, 世界!👋안녕하세요。日本語で書いてみます。123😊\n"

	str, err := ByteListToString(data)
	if err != nil {
		t.Errorf("Expected error to be nil but got %v", err)
	}

	if str != expectedStr {
		t.Errorf("Expected string to be %s but got %s", expectedStr, str)
	}
}

func TestStringToByteList(t *testing.T) {
	data := "Hello, 世界!👋안녕하세요。日本語で書いてみます。123😊\n"
	expectedBytes := []byte{
		0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0xe4, 0xb8, 0x96, 0xe7, 0x95, 0x8c, 0x21, 0xf0, 0x9f, 0x91, 0x8b,
		0xec, 0x95, 0x88, 0xeb, 0x85, 0x95, 0xed, 0x95, 0x98, 0xec, 0x84, 0xb8, 0xec, 0x9a, 0x94, 0xe3, 0x80, 0x82,
		0xe6, 0x97, 0xa5, 0xe6, 0x9c, 0xac, 0xe8, 0xaa, 0x9e, 0xe3, 0x81, 0xa7, 0xe6, 0x9b, 0xb8, 0xe3, 0x81, 0x84,
		0xe3, 0x81, 0xa6, 0xe3, 0x81, 0xbf, 0xe3, 0x81, 0xbe, 0xe3, 0x81, 0x99, 0xe3, 0x80, 0x82, 0x31, 0x32, 0x33,
		0xf0, 0x9f, 0x98, 0x8a, 0x0a,
	}

	bytes, err := StringToByteList(data)
	if err != nil {
		t.Errorf("Expected error to be nil but got %v", err)
	}

	if !bytesEqual(bytes, expectedBytes) {
		t.Errorf("Expected byte slice to be %v but got %v", expectedBytes, bytes)
	}
}

// Helper function for comparing byte slices.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
