package x

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCyclicBuffer(t *testing.T) {
	buf := NewCyclicBuffer(4)
	buf1 := []byte{0, 0, 0, 0}
	buf.Write([]byte{})
	assert.False(t, buf.Compare([]byte{}))
	assert.False(t, buf.Compare([]byte{0, 0, 0}))
	assert.True(t, buf.Compare([]byte{0, 0, 0, 0}))
	assert.Equal(t, 0, buf.Dump(buf1))
	assert.Equal(t, []byte{0, 0, 0, 0}, buf1)
	buf.Write([]byte{1})
	assert.True(t, buf.Compare([]byte{0, 0, 0, 1}))
	buf.Write([]byte{22})
	assert.True(t, buf.Compare([]byte{0, 0, 1, 22}))
	assert.Equal(t, 2, buf.Dump(buf1))
	assert.Equal(t, []byte{1, 22}, buf1[:2])
	buf.Write([]byte{12, 33, 11})
	assert.True(t, buf.Compare([]byte{22, 12, 33, 11}))
	buf.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
	assert.Equal(t, 4, buf.Dump(buf1))
	assert.Equal(t, []byte{11, 12, 13, 14}, buf1)
	assert.True(t, buf.Compare([]byte{11, 12, 13, 14}))
}
