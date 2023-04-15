package core

import (
	"fmt"
)

func ByteListToInt64(data []byte) (int64, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("[ByteListToInt64], len(data) != 4, %#v", data)
	}
	r := int64(data[0])<<24 | int64(data[1])<<16 | int64(data[2])<<8 | int64(data[3])
	return r, nil
}
