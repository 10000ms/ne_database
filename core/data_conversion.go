package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ByteListToInt64 大端字节序
func ByteListToInt64(data []byte) (int64, StandardError) {
	if len(data) != DataByteLengthInt64 {
		return 0, NewDBError(FunctionModelCoreDataConversion, ErrorTypeSystem, ErrorBaseCodeInnerParameterError, fmt.Errorf("[ByteListToInt64], len(data) != 4, %#v", data))
	}
	var val int64
	err := binary.Read(bytes.NewBuffer(data), binary.BigEndian, &val)
	if err != nil {
		return 0, NewDBError(FunctionModelCoreDataConversion, ErrorTypeSystem, ErrorBaseCodeInnerParameterError, err)
	}
	return val, nil
}

// Int64ToByteList 大端字节序，将int64的数据转为[]byte的数据
func Int64ToByteList(data int64) ([]byte, StandardError) {
	b := make([]byte, DataByteLengthInt64)
	binary.BigEndian.PutUint64(b, uint64(data))
	return b, nil
}

func ByteListToString(data []byte) (string, StandardError) {
	str := string(data)
	return str, nil
}

func StringToByteList(data string) ([]byte, StandardError) {
	return []byte(data), nil
}
