package datastructures

import (
	"testing"
)

// --- helpers ---

// assertPanics runs f and fails the test if it does NOT panic.
func assertPanics(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic, but function did not panic", name)
		}
	}()
	f()
}

// --- tests ---

func TestStack_Int_PushTopPopSize(t *testing.T) {
	var s Stack[int]

	if s.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", s.Size())
	}

	// Push a few
	s.Push(10)
	s.Push(20)
	s.Push(30)

	if s.Size() != 3 {
		t.Fatalf("Size() = %d, want 3", s.Size())
	}

	// Top should be last pushed
	top := s.Top()
	if top != 30 {
		t.Fatalf("Top() = %d, want 30", top)
	}

	// Pop one and re-check
	s.Pop()
	if s.Size() != 2 {
		t.Fatalf("Size() after Pop = %d, want 2", s.Size())
	}
	top = s.Top()
	if top != 20 {
		t.Fatalf("Top() = %d, want 20", top)
	}

	// Pop remaining
	s.Pop()
	s.Pop()
	if s.Size() != 0 {
		t.Fatalf("Size() after popping all = %d, want 0", s.Size())
	}
}

func TestStack_String(t *testing.T) {
	var s Stack[string]

	s.Push("a")
	s.Push("b")
	if s.Size() != 2 {
		t.Fatalf("Size() = %d, want 2", s.Size())
	}
	if got := s.Top(); got != "b" {
		t.Fatalf("Top() = %q, want %q", got, "b")
	}
	s.Pop()
	if got := s.Top(); got != "a" {
		t.Fatalf("Top() after Pop = %q, want %q", got, "a")
	}
}

func TestStack_PopOnEmpty_Panics(t *testing.T) {
	var s Stack[int]
	assertPanics(t, "Pop on empty", func() { s.Pop() })
}

func TestStack_TopOnEmpty_Panics(t *testing.T) {
	var s Stack[int]
	assertPanics(t, "Top on empty", func() { _ = s.Top() })
}

// --- benchmarks ---

func BenchmarkStackPush(b *testing.B) {
	var s Stack[int]
	for i := 0; i < b.N; i++ {
		s.Push(i)
	}
}

func BenchmarkStackPushPop(b *testing.B) {
	var s Stack[int]
	// Pre-grow a bit so we measure push/pop more than reslices
	for i := 0; i < 1<<16; i++ {
		s.Push(i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Push(i)
		s.Pop()
	}
}

func BenchmarkStackTop(b *testing.B) {
	var s Stack[int]
	for i := 0; i < 1<<16; i++ {
		s.Push(i)
	}

	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		sink = s.Top()
	}
	_ = sink
}
