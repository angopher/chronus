package x

import (
	"encoding/binary"
	"math/rand"
	"time"
)

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

func fillOnce(data []byte, pos int) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], rand.Uint64())
	cnt := 0
	for pos+cnt < len(data) && cnt < len(buf) {
		data[pos+cnt] = buf[cnt%8]
		cnt++
	}
	return cnt
}

func RandBytes(n int) []byte {
	if n < 1 {
		return []byte{}
	}
	data := make([]byte, n)
	pos := 0
	rand.Seed(time.Now().UnixNano())
	for pos < n {
		pos += fillOnce(data, pos)
	}
	return data
}
