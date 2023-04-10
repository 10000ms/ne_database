package set

import "sync"

type StringSet struct {
	entries *sync.Map
}

func NewStringSet(values ...string) *StringSet {
	var entries sync.Map
	for _, value := range values {
		entries.Store(value, 0)
	}
	return &StringSet{entries: &entries}
}

func (s *StringSet) Add(values ...string) {
	for _, value := range values {
		s.entries.Store(value, 0)
	}
}

func (s *StringSet) Delete(values ...string) {
	for _, value := range values {
		s.entries.Delete(value)
	}
}

func (s *StringSet) Contains(values string) bool {
	_, ok := s.entries.Load(values)
	return ok
}

func (s *StringSet) Members() []string {
	var members []string
	s.entries.Range(func(key, value interface{}) bool {
		v, ok := key.(string)
		if ok {
			members = append(members, v)
		}
		return value != nil
	})
	return members
}
