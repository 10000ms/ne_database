package list

func Int64ListEqual(listA []int64, listB []int64) bool {
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
