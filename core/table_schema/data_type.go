package tableSchema

import (
	"fmt"
	"math"
	"ne_database/core"
)

type DataTypeEnumeration int

const (
	DataTypeInt64 DataTypeEnumeration = iota
	DataTypeString
)

type MetaType interface {
	// GetType 获取值类型
	GetType() DataTypeEnumeration
	// IsNull 判断值是否为空
	IsNull(data []byte) bool
	// GetNull 获取值的对应空值
	GetNull() []byte
	// LogString 返回可读值
	LogString([]byte) string
}

type int64Type struct {
}

func (t *int64Type) GetType() DataTypeEnumeration {
	return DataTypeInt64
}

// IsNull : 0 在数据中是有含义的，这里用最小数来代表Null值
func (t *int64Type) IsNull(data []byte) bool {
	i, err := core.ByteListToInt64(data)
	if err != nil {
		return true
	}
	return i == int64(math.MinInt64)
}

func (t *int64Type) GetNull() []byte {
	nullValue := int64(math.MinInt64)
	r, _ := core.Int64ToByteList(nullValue)
	return r
}

func (t *int64Type) LogString(data []byte) string {
	i, err := core.ByteListToInt64(data)
	if err != nil {
		return "错误的int64类型"
	}
	return fmt.Sprint(i)
}

type stringType struct {
}

func (t *stringType) GetType() DataTypeEnumeration {
	return DataTypeString
}

func (t *stringType) IsNull(data []byte) bool {
	str, _ := core.ByteListToString(data)
	r := []rune(str)
	return string(r[len(r)-1]) == ""
}

func (t *stringType) GetNull() []byte {
	return []byte("")
}

func (t *stringType) LogString(data []byte) string {
	return string(data)
}

var (
	Int64Type  = int64Type{}
	StringType = stringType{}
)
