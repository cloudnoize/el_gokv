package probability

import (
	"fmt"
	"math"
	"testing"
)

func TestGeometricDist(t *testing.T) {
	const (
		p      = 0.25
		trials = 100000
	)
	println("testing with p ", p)
	dist := make(map[float64]int)
	var max float64
	for range trials {
		n := Geometric(p)
		max = math.Max(max, n)
		dist[n]++
	}

	for i := 0; i <= int(max); i++ {
		v, ok := dist[float64(i)]
		if ok {
			fmt.Printf("Key %d, Value=%d\n", i, int(v))
		}
	}
}
