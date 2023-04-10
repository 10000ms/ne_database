package set

import "sync"

type Int64Set struct {
	entries *sync.Map
}

func NewInt64Set(values ...int64) *Int64Set {
	var entries sync.Map
	for _, value := range values {
		entries.Store(value, 0)
	}
	return &Int64Set{entries: &entries}
}

func (s *Int64Set) Add(values ...int64) {
	for _, value := range values {
		s.entries.Store(value, 0)
	}
}

func (s *Int64Set) Delete(values ...int64) {
	for _, value := range values {
		s.entries.Delete(value)
	}
}

func (s *Int64Set) Contains(values int64) bool {
	_, ok := s.entries.Load(values)
	return ok
}

func (s *Int64Set) Members() []int64 {
	var members []int64
	s.entries.Range(func(key, value interface{}) bool {
		v, ok := key.(int64)
		if ok {
			members = append(members, v)
		}
		return value != nil
	})
	return members
}
