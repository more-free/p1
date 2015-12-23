package bitcoin

import "encoding/json"

// TODO make it thread-safe
type IntSet struct {
	set map[int]bool
}

func NewIntSet() *IntSet {
	return &IntSet{
		set: make(map[int]bool),
	}
}

func (s *IntSet) Contains(id int) bool {
	_, exists := s.set[id]
	return exists
}

func (s *IntSet) Add(id int) {
	s.set[id] = true
}

func (s *IntSet) Remove(id int) {
	delete(s.set, id)
}

func (s *IntSet) Keys() []int {
	keys := make([]int, 0)
	for k, _ := range s.set {
		keys = append(keys, k)
	}
	return keys
}

func (m *Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}

func FromBytes(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return &msg, err
}
