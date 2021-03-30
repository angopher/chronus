package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewShardOwner(t *testing.T) {
	assert.Equal(t, uint64(0), newShardOwner(nil))
	assert.Equal(t, uint64(0), newShardOwner(map[uint64]int{}))
	assert.Equal(t, uint64(1), newShardOwner(map[uint64]int{
		5: 1,
		7: 1,
		1: 1,
		3: 1,
	}))
	assert.Equal(t, uint64(5), newShardOwner(map[uint64]int{
		5: 1,
		7: 2,
		1: 2,
		3: 2,
	}))
}
