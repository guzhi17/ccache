package ccache

func BitHighestInt64(n int64) (pos int) {
	if n == 0{ return 0 }
	for i := n >> 1; i != 0; pos++{
		i = i >> 1
	}
	return pos + 1
}

func NormalTo2N(n int64) int64 {
	if n < 1{return 1}
	return 1 << BitHighestInt64(n-1)
}
