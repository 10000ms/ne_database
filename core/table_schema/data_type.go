package tableSchema

import (
	"fmt"
	"math"
	"ne_database/utils"
	"strconv"

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
	// StringToByte 可读值转化为储存值
	StringToByte(string) ([]byte, base.StandardError)
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
		return base.ValueStringErrorValue
	}
	if t.IsNull(data) {
		return base.ValueStringNullValue
	}
	return fmt.Sprint(i)
}

func (t int64Type) StringToByte(data string) ([]byte, base.StandardError) {
	if data == base.ValueStringNullValue {
		return t.GetNull(), nil
	}
	int64Value, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		utils.LogError(fmt.Sprintf("[int64Type.StringToByte.strconv.ParseInt] err: %s", err.Error()))
		return nil, base.NewDBError(base.FunctionModelCoreDTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, err)
	}
	byteValue, er := base.Int64ToByteList(int64Value)
	if er != nil {
		utils.LogDev(string(base.FunctionModelCoreDTableSchema), 10)(fmt.Sprintf("[int64Type.StringToByte.base.Int64ToByteList] err: %s", err.Error()))
		return nil, er
	}
	return byteValue, nil
}

type stringType struct {
}

func (t stringType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeString
}

func (t stringType) IsNull(data []byte) bool {
	return data[len(data)-1] == 0x00
}

func (t stringType) GetNull() []byte {
	return []byte{0x00}
}

func (t stringType) LogString(data []byte) string {
	if t.IsNull(data) {
		return base.ValueStringNullValue
	}
	return string(data)
}

func (t stringType) StringToByte(data string) ([]byte, base.StandardError) {
	if data == base.ValueStringNullValue {
		return t.GetNull(), nil
	}
	byteValue, er := base.StringToByteList(data)
	if er != nil {
		utils.LogDev(string(base.FunctionModelCoreDTableSchema), 10)(fmt.Sprintf("[stringType.StringToByte.base.StringToByteList] err: %s", er.Error()))
		return nil, er
	}
	return byteValue, nil
}

var (
	Int64Type  = int64Type{}
	StringType = stringType{}
)
