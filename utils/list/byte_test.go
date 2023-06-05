package list

import "testing"

func TestByteListEqual(t *testing.T) {
	listA := []byte{1, 2, 3, 4}
	listB := []byte{1, 2, 3, 4}
	if !ByteListEqual(listA, listB) {
		t.Error("Expected true, but got false")
	}

	listC := []byte{1, 2, 3, 4, 5}
	listD := []byte{1, 2, 3, 4}
	if ByteListEqual(listC, listD) {
		t.Error("Expected false, but got true")
	}

	listE := []byte{'a', 'b', 'c'}
	listF := []byte{'a', 'b', 'd'}
	if ByteListEqual(listE, listF) {
		t.Error("Expected false, but got true")
	}
}
