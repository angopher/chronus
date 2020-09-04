package x

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolCapacity(t *testing.T) {
	echoServer := NewEchoServer("tcp", "127.0.0.1:12345")
	err := echoServer.Start()
	assert.Nil(t, err)
	cnt := 0
	p, err := NewBoundedPool(1, 3, time.Second, time.Second, func() (net.Conn, error) {
		cnt++
		return net.Dial("tcp", "127.0.0.1:12345")
	})
	assert.Equal(t, 1, p.Len())
	assert.Equal(t, 1, p.Total())
	assert.Equal(t, 1, cnt)

	conn1, err := p.Get()
	(p.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, p.Len())
	assert.Equal(t, 1, p.Total())
	assert.Equal(t, 1, cnt)

	conn2, err := p.Get()
	(p.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, p.Len())
	assert.Equal(t, 2, p.Total())
	assert.Equal(t, 2, cnt)

	conn3, err := p.Get()
	(p.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, p.Len())
	assert.Equal(t, 3, p.Total())
	assert.Equal(t, 3, cnt)

	_, err = p.Get()
	(p.(*boundedPool)).checkOnce()
	assert.NotNil(t, err)
	assert.Equal(t, 0, p.Len())
	assert.Equal(t, 3, p.Total())
	assert.Equal(t, 3, cnt)

	conn3.Close()
	(p.(*boundedPool)).checkOnce()
	assert.Equal(t, 1, p.Len())
	assert.Equal(t, 3, p.Total())
	assert.Equal(t, 3, cnt)

	conn2.Close()
	conn1.Close()
	(p.(*boundedPool)).checkOnce()
	assert.Equal(t, 3, p.Len())
	assert.Equal(t, 3, p.Total())
	assert.Equal(t, 3, cnt)

	time.Sleep(time.Second)
	// recycle idled connections
	(p.(*boundedPool)).checkOnce()
	(p.(*boundedPool)).checkOnce()
	(p.(*boundedPool)).checkOnce()
	(p.(*boundedPool)).checkOnce()
	assert.Equal(t, 1, p.Len())
	assert.Equal(t, 1, p.Total())

	p.Close()
	echoServer.Close()
}
