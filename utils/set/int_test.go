package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestIntSet(t *testing.T) {
	// 测试NewIntSet方法和Contains方法
	s := NewIntsSet(1, 2, 3)
	if !s.Contain(1) {
		t.Error("IntsSet.Contain failed, expected true but got false")
	}
	if !s.Contain(2) {
		t.Error("IntsSet.Contain failed, expected true but got false")
	}
	if !s.Contain(3) {
		t.Error("IntsSet.Contain failed, expected true but got false")
	}
	if s.Contain(4) {
		t.Error("IntsSet.Contain failed, expected false but got true")
	}

	// 测试Add方法和Members方法
	s.Add(4, 5, 6)
	expectedMembers := []int{1, 2, 3, 4, 5, 6}
	members := s.TotalMember()
	sort.Ints(members)
	if !reflect.DeepEqual(members, expectedMembers) {
		t.Errorf("IntsSet.TotalMember failed, expected %#v but got %#v", expectedMembers, members)
	}

	// 测试Delete方法和Members方法
	s.Delete(1, 3, 5)
	expectedMembers = []int{2, 4, 6}
	members = s.TotalMember()
	sort.Ints(members)
	if !reflect.DeepEqual(members, expectedMembers) {
		t.Errorf("IntsSet.TotalMember failed, expected %#v but got %#v", expectedMembers, members)
	}
}
