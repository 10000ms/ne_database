package tableSchema

import (
	"fmt"
	"math"

	"ne_database/core/base"
)

type MetaType interface {
	// GetType 获取值类型
	GetType() base.DBDataTypeEnumeration
	// IsNull 判断值是否为空
	IsNull(data []byte) bool
	// GetNull 获取值的对应空值
	GetNull() []byte
	// LogString 返回可读值
	LogString([]byte) string
}

type int64Type struct {
}

func (t int64Type) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeInt64
}

// IsNull : 0 在数据中是有含义的，这里用最小数来代表Null值
func (t int64Type) IsNull(data []byte) bool {
	i, err := base.ByteListToInt64(data)
	if err != nil {
		return true
	}
	return i == int64(math.MinInt64)
}

func (t int64Type) GetNull() []byte {
	nullValue := int64(math.MinInt64)
	r, _ := base.Int64ToByteList(nullValue)
	return r
}

func (t int64Type) LogString(data []byte) string {
	i, err := base.ByteListToInt64(data)
	if err != nil {
		return "错误的int64类型"
	}
	return fmt.Sprint(i)
}

type stringType struct {
}

func (t stringType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeString
}

func (t stringType) IsNull(data []byte) bool {
	str, _ := base.ByteListToString(data)
	r := []rune(str)
	return string(r[len(r)-1]) == ""
}

func (t stringType) GetNull() []byte {
	return []byte("")
}

func (t stringType) LogString(data []byte) string {
	return string(data)
}

var (
	Int64Type  = int64Type{}
	StringType = stringType{}
)
