package util

import (
	"fmt"
	"strings"
)

func PadLeft(str string, width int) string {
	if len(str) >= width {
		return str
	}

	return fmt.Sprint(strings.Repeat(" ", width-len(str)), str)
}

func PadRight(str string, width int) string {
	if len(str) >= width {
		return str
	}

	return fmt.Sprint(str, strings.Repeat(" ", width-len(str)))
}
