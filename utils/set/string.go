package set

import "sync"

type StringsSet struct {
	setItems *sync.Map
}

func NewStringsSet(strings ...string) *StringsSet {
	var setItems sync.Map
	for _, v := range strings {
		setItems.Store(v, 0)
	}
	return &StringsSet{setItems: &setItems}
}

func (s *StringsSet) Add(strings ...string) {
	for _, v := range strings {
		s.setItems.Store(v, 0)
	}
}

func (s *StringsSet) Delete(strings ...string) {
	for _, v := range strings {
		s.setItems.Delete(v)
	}
}

func (s *StringsSet) Contain(v string) bool {
	_, ok := s.setItems.Load(v)
	return ok
}

func (s *StringsSet) TotalMember() []string {
	var totalMember []string
	s.setItems.Range(
		func(key, value interface{}) bool {
			v, ok := key.(string)
			if ok {
				totalMember = append(totalMember, v)
			}
			return value != nil
		},
	)
	return totalMember
}
