package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestIntSet(t *testing.T) {
	// 测试NewIntSet方法和Contains方法
	s := NewIntSet(1, 2, 3)
	if !s.Contains(1) {
		t.Error("IntSet.Contains failed, expected true but got false")
	}
	if !s.Contains(2) {
		t.Error("IntSet.Contains failed, expected true but got false")
	}
	if !s.Contains(3) {
		t.Error("IntSet.Contains failed, expected true but got false")
	}
	if s.Contains(4) {
		t.Error("IntSet.Contains failed, expected false but got true")
	}

	// 测试Add方法和Members方法
	s.Add(4, 5, 6)
	expectedMembers := []int{1, 2, 3, 4, 5, 6}
	members := s.Members()
	sort.Ints(members)
	if !reflect.DeepEqual(members, expectedMembers) {
		t.Errorf("IntSet.Members failed, expected %#v but got %#v", expectedMembers, members)
	}

	// 测试Delete方法和Members方法
	s.Delete(1, 3, 5)
	expectedMembers = []int{2, 4, 6}
	members = s.Members()
	sort.Ints(members)
	if !reflect.DeepEqual(members, expectedMembers) {
		t.Errorf("IntSet.Members failed, expected %#v but got %#v", expectedMembers, members)
	}
}
