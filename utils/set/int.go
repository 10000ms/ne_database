package set

import "sync"

type IntSet struct {
	entries *sync.Map
}

func NewIntSet(values ...int) *IntSet {
	var entries sync.Map
	for _, value := range values {
		entries.Store(value, 0)
	}
	return &IntSet{entries: &entries}
}

func (s *IntSet) Add(values ...int) {
	for _, value := range values {
		s.entries.Store(value, 0)
	}
}

func (s *IntSet) Delete(values ...int) {
	for _, value := range values {
		s.entries.Delete(value)
	}
}

func (s *IntSet) Contains(values int) bool {
	_, ok := s.entries.Load(values)
	return ok
}

func (s *IntSet) Members() []int {
	var members []int
	s.entries.Range(func(key, value interface{}) bool {
		v, ok := key.(int)
		if ok {
			members = append(members, v)
		}
		return value != nil
	})
	return members
}
