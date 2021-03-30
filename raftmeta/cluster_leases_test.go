package raftmeta_test

import (
	"testing"
	"time"

	"github.com/angopher/chronus/raftmeta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestLease(t *testing.T) {
	leases := raftmeta.NewClusterLeases(time.Second, zap.NewNop())
	KEY1 := "k1"
	NODE1 := uint64(1)
	NODE2 := uint64(2)

	l, err := leases.Acquire(KEY1, NODE1, 0)
	assert.Equal(t, raftmeta.ErrSkipExpired, err)
	assert.Nil(t, l)

	l, err = leases.Acquire(KEY1, NODE1, time.Now().UnixNano()/1e6)
	assert.Nil(t, err)
	assert.NotNil(t, l)

	l, err = leases.Acquire(KEY1, NODE2, time.Now().UnixNano()/1e6)
	assert.Equal(t, raftmeta.ErrAcquiredByOther, err)
	assert.NotNil(t, l)

	assert.True(t, leases.ShouldTry(KEY1, NODE1))
	assert.False(t, leases.ShouldTry(KEY1, NODE2))

	l = leases.Get(KEY1)
	assert.NotNil(t, l)
	assert.Equal(t, KEY1, l.Name)
	assert.Equal(t, NODE1, l.Owner)
}
