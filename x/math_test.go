package x

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert.Equal(t, 0, Max(-1, 0, -33, 0, -99))
	assert.Equal(t, 3, Max(-1, 0, -33, 3, -99))
}

func TestMin(t *testing.T) {
	assert.Equal(t, -99, Min(-1, 0, -33, 0, -99))
	assert.Equal(t, -33, Min(-1, 0, -33, 3, 99))
}

func TestRandBytes(t *testing.T) {
	assert.Equal(t, 0, len(RandBytes(0)))
	assert.Equal(t, 2, len(RandBytes(2)))
	assert.Equal(t, 7, len(RandBytes(7)))
	assert.Equal(t, 8, len(RandBytes(8)))
	assert.Equal(t, 17, len(RandBytes(17)))
}
