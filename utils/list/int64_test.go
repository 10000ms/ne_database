package list

import "testing"

func TestInt64ListEqual(t *testing.T) {
	listA := []int64{1, 2, 3, 4}
	listB := []int64{1, 2, 3, 4}
	if !Int64ListEqual(listA, listB) {
		t.Error("Expected true, but got false")
	}

	listC := []int64{1, 2, 3, 4, 5}
	listD := []int64{1, 2, 3, 4}
	if Int64ListEqual(listC, listD) {
		t.Error("Expected false, but got true")
	}

	listE := []int64{0, -1, 0x7fffffffffffffff}
	listF := []int64{0, -1, 0x7ffffffffffffffe}
	if Int64ListEqual(listE, listF) {
		t.Error("Expected false, but got true")
	}
}
