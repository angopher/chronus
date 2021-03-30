package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPadLeft(t *testing.T) {
	assert.Equal(t, "   123", PadLeft("123", 6))
	assert.Equal(t, "123", PadLeft("123", 3))
	assert.Equal(t, "123", PadLeft("123", 2))
	assert.Equal(t, "123", PadLeft("123", 0))
	assert.Equal(t, "123", PadLeft("123", -1))
	assert.Equal(t, " 123", PadLeft("123", 4))

	assert.Equal(t, " ", PadLeft("", 1))
	assert.Equal(t, "   ", PadLeft("", 3))
}

func TestPadRight(t *testing.T) {
	assert.Equal(t, "123   ", PadRight("123", 6))
	assert.Equal(t, "123", PadRight("123", 3))
	assert.Equal(t, "123", PadRight("123", 2))
	assert.Equal(t, "123", PadRight("123", 0))
	assert.Equal(t, "123", PadRight("123", -1))
	assert.Equal(t, "123 ", PadRight("123", 4))

	assert.Equal(t, " ", PadRight("", 1))
	assert.Equal(t, "   ", PadRight("", 3))
}
