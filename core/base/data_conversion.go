package base

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
ByteListToInt64 大端字节序

计算逻辑
data := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
int64 = (+/-)(0 * 256 ** 7) + (1 * 256 ** 6) + (2 * 256 ** 5) + (3 * 256 ** 4) + (4 * 256 ** 3) + (5 * 256 ** 2) + (6 * 256 ** 1) + (7 * 256 ** 0)
另外考虑到正负数需要，根据二进制补码的表示方法来确定正负号，并将字节数组转换成相应的整数值

获取最高位:
b := byte(0x8F) // 示例字节，其二进制形式为 10001111
msb := ((b & (1 << 7)) >> 7) == 1

	if msb {
	    fmt.Println("字节的最高有效位为1")
	} else {

	    fmt.Println("字节的最高有效位为0")
	}

若要计算n位数补码二进制对应的十进制，需要知道每位数对应的数字，除了最高比特外，其他比特的对应数字均和一般二进制相同，即第i位数表示数字2i−1。但最高比特若为1时，其表示数字为 -2n−1，因此若用此方式计算0000 0101表示的数字，其结果为：

1111 1011 (−5) = −128 + 64 + 32 + 16 + 8 + 0 + 2 + 1 = (−27 + 26 + ...) = −5
*/
func ByteListToInt64(data []byte) (int64, StandardError) {
	if len(data) != DataByteLengthInt64 {
		return 0, NewDBError(FunctionModelCoreDataConversion, ErrorTypeSystem, ErrorBaseCodeInnerParameterError, fmt.Errorf("[ByteListToInt64], len(data) != %d, %#v", DataByteLengthInt64, data))
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
