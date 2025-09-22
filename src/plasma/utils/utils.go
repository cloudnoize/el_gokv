package utils

func IsPowerOf2(n uint64) bool {
	return n&(n-1) == 0
}
