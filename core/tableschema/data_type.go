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
	// Like 数据对比: like
	Like([]byte, []byte) (bool, base.StandardError)
	// ILike 数据对比: iLike
	ILike([]byte, []byte) (bool, base.StandardError)
	// IsNull 数据对比: 是否为Null
	IsNull([]byte) (bool, base.StandardError)
}

type bigIntType struct {
}

func (t bigIntType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeBigInt
}

func (t bigIntType) StringValue(data []byte) string {
	i, err := base.ByteListToInt64(data)
	if err != nil {
		return base.ValueStringErrorValue
	}
	return fmt.Sprint(i)
}

func (t bigIntType) StringToByte(data string) ([]byte, base.StandardError) {
	int64Value, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		utils.LogError(fmt.Sprintf("[bigIntType.StringToByte.strconv.ParseInt] err: %s", err.Error()))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, err)
	}
	byteValue, er := base.Int64ToByteList(int64Value)
	if er != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.StringToByte.base.Int64ToByteList] err: %s", err.Error()))
		return nil, er
	}
	return byteValue, nil
}

func (t bigIntType) LengthPadding(waitHandleData []byte, length int) ([]byte, base.StandardError) {
	if len(waitHandleData) != base.DataByteLengthInt64 {
		utils.LogError(fmt.Sprintf("[bigIntType.LengthPadding] err: int64 数据长度不对"))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("int64 数据长度不对"))
	}
	return waitHandleData, nil
}

func (t bigIntType) TrimRaw(data []byte) []byte {
	if data == nil {
		return make([]byte, 0)
	} else {
		return data
	}
}

func (t bigIntType) Greater(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Greater.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Greater.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 > value2, nil
}

func (t bigIntType) Equal(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Equal.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Equal.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 == value2, nil
}

func (t bigIntType) Less(data1 []byte, data2 []byte) (bool, base.StandardError) {
	value1, err := base.ByteListToInt64(data1)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Less.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	value2, err := base.ByteListToInt64(data2)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.Less.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value1 < value2, nil
}

func (t bigIntType) Like(originValue []byte, compareValue []byte) (bool, base.StandardError) {
	// 数字没有Like
	return false, nil
}

func (t bigIntType) ILike(originValue []byte, compareValue []byte) (bool, base.StandardError) {
	// 数字没有Like
	return false, nil
}

func (t bigIntType) IsNull(checkValue []byte) (bool, base.StandardError) {
	value, err := base.ByteListToInt64(checkValue)
	if err != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[bigIntType.IsNull.base.ByteListToInt64] err: %s", err.Error()))
		return false, err
	}
	return value == 0, nil
}

type charType struct {
}

func (t charType) GetType() base.DBDataTypeEnumeration {
	return base.DBDataTypeChar
}

func (t charType) StringValue(data []byte) string {
	return string(data)
}

func (t charType) StringToByte(data string) ([]byte, base.StandardError) {
	byteValue, er := base.StringToByteList(data)
	if er != nil {
		utils.LogDev(string(base.FunctionModelCoreTableSchema))(fmt.Sprintf("[charType.StringToByte.base.StringToByteList] err: %s", er.Error()))
		return nil, er
	}
	return byteValue, nil
}

func (t charType) LengthPadding(waitHandleData []byte, length int) ([]byte, base.StandardError) {
	if len(waitHandleData) == length {
		return waitHandleData, nil
	}
	if len(waitHandleData) > length {
		utils.LogError(fmt.Sprintf("[charType.LengthPadding] err: string 数据长度不对"))
		return nil, base.NewDBError(base.FunctionModelCoreTableSchema, base.ErrorTypeInput, base.ErrorBaseCodeParameterError, fmt.Errorf("string 数据长度不对"))
	}
	padding := make([]byte, length-len(waitHandleData))
	return append(waitHandleData, padding...), nil
}

func (t charType) TrimRaw(data []byte) []byte {
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

func (t charType) Greater(data1 []byte, data2 []byte) (bool, base.StandardError) {
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

func (t charType) Equal(data1 []byte, data2 []byte) (bool, base.StandardError) {
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

func (t charType) Less(data1 []byte, data2 []byte) (bool, base.StandardError) {
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

func (t charType) Like(originValue []byte, compareValue []byte) (bool, base.StandardError) {
	var (
		origin  = ""
		compare = string(compareValue)
	)
	if originValue[0] == base.SymbolDataComparatorLikePlaceholder && originValue[len(originValue)-1] == base.SymbolDataComparatorLikePlaceholder {
		if len(originValue) > 2 {
			origin = string(originValue[1 : len(originValue)-2])
		}
	} else if originValue[len(originValue)-1] == base.SymbolDataComparatorLikePlaceholder {
		if len(originValue) > 1 {
			origin = string(originValue[:len(originValue)-2])
		}
	} else if originValue[0] == base.SymbolDataComparatorLikePlaceholder {
		if len(originValue) > 1 {
			origin = string(originValue[1:])
		}
	}
	return strings.Contains(compare, origin), nil
}

func (t charType) ILike(originValue []byte, compareValue []byte) (bool, base.StandardError) {
	origin := string(originValue)
	compare := string(compareValue)
	newOrigin := strings.ToLower(origin)
	newCompare := strings.ToLower(compare)
	return t.Like([]byte(newOrigin), []byte(newCompare))
}

func (t charType) IsNull(checkValue []byte) (bool, base.StandardError) {
	empty := checkValue == nil || len(checkValue) == 0
	nullString := len(checkValue) == 1 && checkValue[0] == NullStringByte
	return empty || nullString, nil
}

var (
	BigIntType = bigIntType{}
	CharType   = charType{}
)
