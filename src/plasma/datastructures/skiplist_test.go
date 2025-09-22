package datastructures

import (
	"bytes"
	"sort"
	"testing"
)

// assertPanics runs f and fails the test if it does NOT panic.
func assertPanicsDup(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic, but function did not panic", name)
		}
	}()
	f()
}

// --- tests ---

func TestSkipList_capacityPowerOf2(t *testing.T) {
	assertPanicsDup(t, "Power of 2", func() { NewSkipList(1000, 0.5) })
	ns := NewSkipList(1024, 0.5)

	if ns.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", ns.Size())
	}
}

func TestSkipList_putAndGet(t *testing.T) {
	sl := NewSkipList(1024, 0.5)
	key := []byte("key")
	version := 0
	value := []byte("Val")
	sl.Put(key, value, uint64(version))
	if sl.Size() != 1 {
		t.Fatalf("Expected size 1 , actual %d", sl.Size())
	}
	if v, ok := sl.Get([]byte("doesn't exist")); ok {
		t.Fatalf("Expected not to get a value but received %x", v)
	}
	var out []byte
	var ok bool
	if out, ok = sl.Get(key); !ok {
		t.Fatalf("Expected to get a value but didn't")
	}
	if !bytes.Equal(out, value) {
		t.Fatalf("expected %s but got %s", value, out)
	}

}

func TestSkipList_severalPutsAndGets(t *testing.T) {
	sl := NewSkipList(1024, 0.5)

	elems := [][]byte{
		[]byte("eee"),
		[]byte("bbb"),
		[]byte("ccc"),
	}

	sortedElems := make([][]byte, len(elems))
	copy(sortedElems, elems)
	sort.Slice(sortedElems, func(i, j int) bool {
		return bytes.Compare(sortedElems[i], sortedElems[j]) < 0
	})

	for _, e := range elems {
		sl.Put(e, e, 0)
	}

	if sl.Size() != 3 {
		t.Fatalf("Expected 3 got %d", sl.Size())
	}

	itr := sl.Iterator()
	idx := 0
	for itr.Dref() != nil {
		if !bytes.Equal(itr.Dref().Value, sortedElems[idx]) {
			t.Fatalf("Expected %x got %x", sortedElems[idx], itr.Dref().Value)
		}
		idx++
		itr.Next()
	}

	for _, key := range elems {
		if val, ok := sl.Get(key); !ok || !bytes.Equal(key, val) {
			t.Fatalf("Expected %d got %d", key, val)
		}
	}
}
