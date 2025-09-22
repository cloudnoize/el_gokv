package probability

import "math/rand"

func Geometric(p float64) float64 {
	var k float64
	for p > rand.Float64() {
		k++
	}
	return k
}
