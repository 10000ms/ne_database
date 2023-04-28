package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestNewStringSetEmpty(t *testing.T) {
	set := NewStringSet()
	if len(set.Members()) != 0 {
		t.Errorf("NewStringSet() did not create an empty set")
	}
}

func TestStringSetAdd(t *testing.T) {
	set := NewStringSet("foo", "bar")
	set.Add("baz", "qux")
	expected := []string{"bar", "baz", "foo", "qux"}
	members := set.Members()
	sort.Strings(members)
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("StringSet.Add() failed to add elements to the set")
	}
}

func TestStringSetDelete(t *testing.T) {
	set := NewStringSet("foo", "bar", "baz", "qux")
	set.Delete("bar", "qux")
	expected := []string{"baz", "foo"}
	members := set.Members()
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("StringSet.Delete() failed to remove elements from the set")
	}
}

func TestStringSetContains(t *testing.T) {
	set := NewStringSet("foo", "bar", "baz", "qux")
	if !set.Contains("baz") {
		t.Errorf("StringSet.Contains() failed to find an existing element in the set")
	}
	if set.Contains("hello") {
		t.Errorf("StringSet.Contains() incorrectly found a non-existing element in the set")
	}
}

func TestStringSetMembers(t *testing.T) {
	set := NewStringSet("foo", "bar", "baz", "qux")
	expected := []string{"bar", "baz", "foo", "qux"}
	members := set.Members()
	sort.Strings(members)
	if !reflect.DeepEqual(members, expected) {
		t.Errorf("StringSet.Members() failed to return all elements in the set")
	}
}
