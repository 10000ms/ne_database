package set

import "sync"

type Int64sSet struct {
	setItems *sync.Map
}

func NewInt64sSet(int64s ...int64) *Int64sSet {
	var setItems sync.Map
	for _, v := range int64s {
		setItems.Store(v, 0)
	}
	return &Int64sSet{setItems: &setItems}
}

func (s *Int64sSet) Add(int64s ...int64) {
	for _, v := range int64s {
		s.setItems.Store(v, 0)
	}
}

func (s *Int64sSet) Delete(int64s ...int64) {
	for _, v := range int64s {
		s.setItems.Delete(v)
	}
}

func (s *Int64sSet) Contain(v int64) bool {
	_, ok := s.setItems.Load(v)
	return ok
}

func (s *Int64sSet) TotalMember() []int64 {
	var totalMember []int64
	s.setItems.Range(
		func(key, value interface{}) bool {
			v, ok := key.(int64)
			if ok {
				totalMember = append(totalMember, v)
			}
			return value != nil
		},
	)
	return totalMember
}

func (s *Int64sSet) Difference(otherSet *Int64sSet) *Int64sSet {
	r := NewInt64sSet()
	for _, v := range s.TotalMember() {
		if !otherSet.Contain(v) {
			r.Add(v)
		}
	}
	return r
}
