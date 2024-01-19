package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestNewStringSetEmpty(t *testing.T) {
	set := NewStringsSet()
	if len(set.TotalMember()) != 0 {
		t.Errorf("NewStringsSet() did not create an empty set")
	}
}

func TestStringSetAdd(t *testing.T) {
	set := NewStringsSet("foo", "bar")
	set.Add("baz", "qux")
	expected := []string{"bar", "baz", "foo", "qux"}
	members := set.TotalMember()
	sort.Strings(members)
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("StringsSet.Add() failed to add elements to the set")
	}
}

func TestStringSetDelete(t *testing.T) {
	set := NewStringsSet("foo", "bar", "baz", "qux")
	set.Delete("bar", "qux")
	expected := []string{"baz", "foo"}
	members := set.TotalMember()
	if len(members) != len(expected) {
		t.Errorf("StringsSet.Delete() failed to remove elements from the set, got: %v, expected: %v", members, expected)
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
		t.Errorf("StringsSet.Delete() failed to remove elements from the set, got: %v, expected: %v", members, expected)
	}
}

func TestStringSetContains(t *testing.T) {
	set := NewStringsSet("foo", "bar", "baz", "qux")
	if !set.Contain("baz") {
		t.Errorf("StringsSet.Contain() failed to find an existing element in the set")
	}
	if set.Contain("hello") {
		t.Errorf("StringsSet.Contain() incorrectly found a non-existing element in the set")
	}
}

func TestStringSetMembers(t *testing.T) {
	set := NewStringsSet("foo", "bar", "baz", "qux")
	expected := []string{"bar", "baz", "foo", "qux"}
	members := set.TotalMember()
	sort.Strings(members)
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("StringsSet.TotalMember() failed to return all elements in the set")
	}
}
