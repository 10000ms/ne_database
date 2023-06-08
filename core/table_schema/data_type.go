package tableSchema

import (
	"fmt"
	"math"
	"strconv"

	"ne_database/core/base"
	"ne_database/utils"
	"ne_database/utils/list"
)

const (
	NullStringByte = 0x00
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
	// LengthPadding 长度填充
	LengthPadding([]byte, int) ([]byte, base.StandardError)
	// TrimRaw 可变长度数据类型进行修整
	TrimRaw([]byte) []byte
}

type int64Type struct {
}

func (t int64Type) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeInt64
}

// IsNull : 0 在数据中是有含义的，这里用最小数来代表Null值
func (t int64Type) IsNull(data []byte) bool {
	// 字符串的零值也认为是零
	nullValue, _ := base.StringToByteList(base.ValueStringNullValue)
	if len(data) >= len(nullValue) && list.ByteListEqual(data[0:len(nullValue)], nullValue) {
		return true
	}
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
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, err)
	}
	byteValue, er := base.Int64ToByteList(int64Value)
	if er != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.StringToByte.base.Int64ToByteList] err: %s", err.Error()))
		return nil, er
	}
	return byteValue, nil
}

func (t int64Type) LengthPadding(waitHandleData []byte, length int) ([]byte, base.StandardError) {
	if len(waitHandleData) != base.DataByteLengthInt64 {
		utils.LogError(fmt.Sprintf("[int64Type.LengthPadding] err: int64 数据长度不对"))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("int64 数据长度不对"))
	}
	return waitHandleData, nil
}

func (t int64Type) TrimRaw(data []byte) []byte {
	if data == nil {
		return make([]byte, 0)
	} else {
		return data
	}
}

type stringType struct {
}

func (t stringType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeString
}

func (t stringType) IsNull(data []byte) bool {
	// 字符串的零值也认为是零
	nullValue, _ := base.StringToByteList(base.ValueStringNullValue)
	if len(data) >= len(nullValue) && list.ByteListEqual(data[0:len(nullValue)], nullValue) {
		return true
	}
	if data != nil && len(data) > 0 {
		return data[0] == NullStringByte
	}
	return true
}

func (t stringType) GetNull() []byte {
	return []byte{NullStringByte}
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
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[stringType.StringToByte.base.StringToByteList] err: %s", er.Error()))
		return nil, er
	}
	return byteValue, nil
}

func (t stringType) LengthPadding(waitHandleData []byte, length int) ([]byte, base.StandardError) {
	if len(waitHandleData) == length {
		return waitHandleData, nil
	}
	if len(waitHandleData) > length {
		utils.LogError(fmt.Sprintf("[stringType.LengthPadding] err: string 数据长度不对"))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("string 数据长度不对"))
	}
	padding := make([]byte, length-len(waitHandleData))
	return append(waitHandleData, padding...), nil
}

func (t stringType) TrimRaw(data []byte) []byte {
	if data == nil {
		return make([]byte, 0)
	}
	endOffset := -1
	for i, v := range data {
		if v == NullStringByte {
			endOffset = i
			break
		}
	}
	if endOffset == -1 {
		return data
	} else if endOffset == 0 {
		return make([]byte, 0)
	} else {
		return data[:endOffset]
	}
}

var (
	Int64Type  = int64Type{}
	StringType = stringType{}
)
