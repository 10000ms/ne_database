package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestNewInt64SetEmpty(t *testing.T) {
	set := NewInt64Set()
	if len(set.Members()) != 0 {
		t.Errorf("NewInt64Set() did not create an empty set")
	}
}

func TestInt64SetAdd(t *testing.T) {
	set := NewInt64Set(1, 2)
	set.Add(3, 4)
	expected := []int64{1, 2, 3, 4}
	members := set.Members()
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("Int64Set.Add() failed to add elements to the set")
	}
}

func TestInt64SetDelete(t *testing.T) {
	set := NewInt64Set(1, 2, 3, 4)
	set.Delete(2, 4)
	expected := []int64{1, 3}
	members := set.Members()
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("Int64Set.Delete() failed to remove elements from the set")
	}
}

func TestInt64SetContains(t *testing.T) {
	set := NewInt64Set(1, 2, 3, 4)
	if !set.Contains(3) {
		t.Errorf("Int64Set.Contains() failed to find an existing element in the set")
	}
	if set.Contains(5) {
		t.Errorf("Int64Set.Contains() incorrectly found a non-existing element in the set")
	}
}

func TestInt64SetMembers(t *testing.T) {
	set := NewInt64Set(1, 2, 3, 4)
	expected := []int64{1, 2, 3, 4}
	members := set.Members()
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("Int64Set.Members() failed to return all elements in the set")
	}
}
