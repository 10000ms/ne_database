package list

func ByteListEqual(listA []byte, listB []byte) bool {
	if len(listA) != len(listB) {
		return false
	}
	for i, v := range listA {
		if v != listB[i] {
			return false
		}
	}
	return true
}
