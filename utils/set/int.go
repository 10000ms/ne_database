package set

import "sync"

type IntsSet struct {
	setItems *sync.Map
}

func NewIntsSet(ints ...int) *IntsSet {
	var setItems sync.Map
	for _, v := range ints {
		setItems.Store(v, 0)
	}
	return &IntsSet{setItems: &setItems}
}

func (s *IntsSet) Add(ints ...int) {
	for _, v := range ints {
		s.setItems.Store(v, 0)
	}
}

func (s *IntsSet) Delete(ints ...int) {
	for _, v := range ints {
		s.setItems.Delete(v)
	}
}

func (s *IntsSet) Contain(i int) bool {
	_, ok := s.setItems.Load(i)
	return ok
}

func (s *IntsSet) TotalMember() []int {
	var totalMember []int
	s.setItems.Range(
		func(key, value interface{}) bool {
			v, ok := key.(int)
			if ok {
				totalMember = append(totalMember, v)
			}
			return value != nil
		},
	)
	return totalMember
}
