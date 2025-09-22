package datastructures

import "github.com/cloudnoize/el_gokv/src/plasma/utils"

type Stack[T any] struct {
	stack []T
	idx   int
}

func (s *Stack[T]) Size() int {
	return s.idx
}

func (s *Stack[T]) Push(e T) {
	if s.idx == len(s.stack) {
		s.stack = append(s.stack, e)
	} else {
		s.stack[s.idx] = e
	}
	s.idx++
}

func (s *Stack[T]) Pop() {
	utils.Assert(s.Size() > 0, "Trying to pop from empty stack")
	s.idx--
}

func (s *Stack[T]) Top() T {
	utils.Assert(s.Size() > 0, "Trying to pop from empty stack")
	return s.stack[s.idx-1]
}
