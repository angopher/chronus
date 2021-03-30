package x

import (
	"os"
)

type ExistStat int

const (
	ExistUnknow ExistStat = iota
	NotExisted
	ExistFile
	ExistDir
)

// Exists returns the existed stat for specific path
func Exists(ospath string) ExistStat {
	stat, err := os.Stat(ospath)
	if err != nil {
		if os.IsNotExist(err) {
			return NotExisted
		}
		return ExistUnknow
	}
	if stat.IsDir() {
		return ExistDir
	}
	return ExistFile
}
