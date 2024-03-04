package tableschema

import (
	"fmt"
	"strconv"
	"strings"

	"ne_database/core/base"
	"ne_database/utils"
)

const (
	NullStringByte = 0x00
)

type MetaType interface {
	// GetType 获取值类型
	GetType() base.DBDataTypeEnumeration
	// StringValue 返回可读值
	StringValue([]byte) string
	// StringToByte 可读值转化为储存值
	StringToByte(string) ([]byte, base.StandardError)
	// LengthPadding 长度填充
	LengthPadding([]byte, int) ([]byte, base.StandardError)
	// TrimRaw 可变长度数据类型进行修整
	TrimRaw([]byte) []byte
	// Greater 数据对比: 大于
	Greater([]byte, []byte) (bool, base.StandardError)
	// Equal 数据对比: 等于
	Equal([]byte, []byte) (bool, base.StandardError)
	// Less 数据对比: 小于
	Less([]byte, []byte) (bool, base.StandardError)
}

type int64Type struct {
}

func (t int64Type) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeInt64
}

func (t int64Type) StringValue(data []byte) string {
	i, err := base.ByteListToInt64(data)
	if err != nil {
		return base.ValueStringErrorValue
	}
	return fmt.Sprint(i)
}

func (t int64Type) StringToByte(data string) ([]byte, base.StandardError) {
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

func (t int64Type) Greater(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 > value2, nil
}

func (t int64Type) Equal(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 == value2, nil
}

func (t int64Type) Less(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema), 10)(fmt.Sprintf("[int64Type.int64Type.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 < value2, nil
}

type stringType struct {
}

func (t stringType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeString
}

func (t stringType) StringValue(data []byte) string {
	return string(data)
}

func (t stringType) StringToByte(data string) ([]byte, base.StandardError) {
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

func (t stringType) Greater(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1 := string(data1)
	value2 := string(data2)
	result := strings.Compare(value1, value2)

	switch result {
	case -1:
		return false, nil
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, nil
}

func (t stringType) Equal(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1 := string(data1)
	value2 := string(data2)
	result := strings.Compare(value1, value2)

	switch result {
	case -1:
		return false, nil
	case 0:
		return true, nil
	case 1:
		return false, nil
	}
	return false, nil
}

func (t stringType) Less(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1 := string(data1)
	value2 := string(data2)
	result := strings.Compare(value1, value2)

	switch result {
	case -1:
		return true, nil
	case 0:
		return false, nil
	case 1:
		return false, nil
	}
	return false, nil
}

var (
	Int64Type  = int64Type{}
	StringType = stringType{}
)
