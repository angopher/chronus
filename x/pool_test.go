package x

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func dumpStatistics(stat PoolStatistics) {
	fmt.Println("Active/Idle/Capacity:", fmt.Sprintf("%d/%d/%d", stat.Active, stat.Idle, stat.Capacity))
	fmt.Println("Get Success:", fmt.Sprintf("%d times, %d ms", stat.GetSuccessCnt, stat.GetSuccessMillis))
	fmt.Println("Get Failure:", fmt.Sprintf("%d times, %d ms", stat.GetFailureCnt, stat.GetFailureMillis))
	fmt.Println("Return:", stat.ReturnCnt)
	fmt.Println("Close:", stat.CloseCnt)
}

func TestPoolCapacity(t *testing.T) {
	echoServer := NewEchoServer("tcp", "127.0.0.1:12345")
	err := echoServer.Start()
	assert.Nil(t, err)
	cnt := 0
	pool, err := NewBoundedPool(1, 3, time.Second, time.Second, func() (net.Conn, error) {
		cnt++
		return net.Dial("tcp", "127.0.0.1:12345")
	})
	assert.Equal(t, 1, pool.Len())
	assert.Equal(t, 1, pool.Total())
	assert.Equal(t, 1, cnt)

	// normal get
	conn1, err := pool.Get()
	(pool.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, 1, pool.Total())
	assert.Equal(t, 1, cnt)

	// normal get
	conn2, err := pool.Get()
	(pool.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, 2, pool.Total())
	assert.Equal(t, 2, cnt)

	// normal get
	conn3, err := pool.Get()
	(pool.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, 3, pool.Total())
	assert.Equal(t, 3, cnt)

	// cannot get more, failure + 1
	_, err = pool.Get()
	(pool.(*boundedPool)).checkOnce()
	assert.NotNil(t, err)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, 3, pool.Total())
	assert.Equal(t, 3, cnt)

	stat := pool.Statistics()
	assert.Equal(t, 3, stat.Active)
	assert.Equal(t, 3, stat.Capacity)
	assert.Equal(t, uint64(3), stat.GetSuccessCnt)
	assert.Equal(t, uint64(1), stat.GetFailureCnt)
	assert.Equal(t, uint64(0), stat.ReturnCnt)
	assert.Equal(t, uint64(0), stat.CloseCnt)

	// close 1 connection
	conn3.Close()
	(pool.(*boundedPool)).checkOnce()
	assert.Equal(t, 1, pool.Len())
	assert.Equal(t, 3, pool.Total())
	assert.Equal(t, 3, cnt)

	// close remained 2 connections
	conn2.Close()
	conn1.Close()
	(pool.(*boundedPool)).checkOnce()
	assert.Equal(t, 3, pool.Len())
	assert.Equal(t, 3, pool.Total())
	assert.Equal(t, 3, cnt)

	time.Sleep(time.Second)
	// recycle idled connections
	(pool.(*boundedPool)).checkOnce()
	(pool.(*boundedPool)).checkOnce()
	(pool.(*boundedPool)).checkOnce()
	(pool.(*boundedPool)).checkOnce()
	assert.Equal(t, 1, pool.Len())
	assert.Equal(t, 1, pool.Total())

	stat = pool.Statistics()
	assert.Equal(t, 0, stat.Active)
	assert.Equal(t, 3, stat.Capacity)
	assert.Equal(t, uint64(3), stat.GetSuccessCnt)
	assert.Equal(t, uint64(1), stat.GetFailureCnt)
	assert.Equal(t, uint64(3), stat.ReturnCnt)
	assert.Equal(t, uint64(2), stat.CloseCnt)

	pool.Close()
	stat = pool.Statistics()
	assert.Equal(t, uint64(3), stat.ReturnCnt)
	assert.Equal(t, uint64(3), stat.CloseCnt)
	echoServer.Close()
}

func TestPoolCloseBeforeConnClose(t *testing.T) {
	echoServer := NewEchoServer("tcp", "127.0.0.1:12345")
	err := echoServer.Start()
	assert.Nil(t, err)
	cnt := 0
	pool, err := NewBoundedPool(1, 3, time.Second, time.Second, func() (net.Conn, error) {
		cnt++
		return net.Dial("tcp", "127.0.0.1:12345")
	})

	// normal get
	conn, err := pool.Get()
	(pool.(*boundedPool)).checkOnce()
	assert.Nil(t, err)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, 1, pool.Total())
	assert.Equal(t, 1, cnt)

	pool.Close()
	stat := pool.Statistics()
	assert.Equal(t, uint64(0), stat.ReturnCnt)
	assert.Equal(t, uint64(0), stat.CloseCnt)

	conn.Close()
	stat = pool.Statistics()
	assert.Equal(t, uint64(1), stat.ReturnCnt)
	assert.Equal(t, uint64(1), stat.CloseCnt)

	echoServer.Close()
}
