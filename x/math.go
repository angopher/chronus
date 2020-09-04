package x

func Max(args ...int) int {
	result := args[0]
	for i := 1; i < len(args); i++ {
		if args[i] > result {
			result = args[i]
		}
	}
	return result
}

func Min(args ...int) int {
	result := args[0]
	for i := 1; i < len(args); i++ {
		if args[i] < result {
			result = args[i]
		}
	}
	return result
}
