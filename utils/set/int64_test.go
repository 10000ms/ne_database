package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestNewInt64SetEmpty(t *testing.T) {
	set := NewInt64sSet()
	if len(set.TotalMember()) != 0 {
		t.Errorf("NewInt64sSet() did not create an empty set")
	}
}

func TestInt64SetAdd(t *testing.T) {
	set := NewInt64sSet(1, 2)
	set.Add(3, 4)
	expected := []int64{1, 2, 3, 4}
	members := set.TotalMember()
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("Int64sSet.Add() failed to add elements to the set")
	}
}

func TestInt64SetDelete(t *testing.T) {
	set := NewInt64sSet(1, 2, 3, 4)
	set.Delete(2, 4)
	expected := []int64{1, 3}
	members := set.TotalMember()
	if len(members) != len(expected) {
		t.Errorf("Int64sSet.Delete() failed to remove elements from the set, got: %v, expected: %v", members, expected)
	}
	check0 := false
	check1 := false
	for _, s := range members {
		if s == expected[0] {
			check0 = true
		} else if s == expected[1] {
			check1 = true
		}
	}
	if !check0 || !check1 {
		t.Errorf("Int64sSet.Delete() failed to remove elements from the set, got: %v, expected: %v", members, expected)
	}
}

func TestInt64SetContains(t *testing.T) {
	set := NewInt64sSet(1, 2, 3, 4)
	if !set.Contain(3) {
		t.Errorf("Int64sSet.Contain() failed to find an existing element in the set")
	}
	if set.Contain(5) {
		t.Errorf("Int64sSet.Contain() incorrectly found a non-existing element in the set")
	}
}

func TestInt64SetMembers(t *testing.T) {
	set := NewInt64sSet(1, 2, 3, 4)
	expected := []int64{1, 2, 3, 4}
	members := set.TotalMember()
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("Int64sSet.TotalMember() failed to return all elements in the set")
	}
}
