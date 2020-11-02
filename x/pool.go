package x

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

type PoolStatistics struct {
	Active, Idle, Capacity          int
	GetSuccessCnt, GetSuccessMillis uint64
	GetFailureCnt, GetFailureMillis uint64
	ReturnCnt, CloseCnt             uint64
}

type ConnPool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error.
	Get() (PooledConn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns current idled connection count in pool
	Len() int
	// Total returns total connection count managed by pool
	Total() int
	Statistics() PoolStatistics
}

type PooledConn interface {
	net.Conn
	MarkUnusable()
}

// boundedPool implements the Pool interface based on buffered channels.
type boundedPool struct {
	// storage for our net.Conn connections
	mu sync.RWMutex
	// Using a channel to hold idled connections in pool is not a good idea
	//	for closing idled ones later. We may change it into an array to make
	//	more senses.
	conns chan *pooledConn

	closer chan int
	wg     sync.WaitGroup

	waitTimeout time.Duration
	idleTimeout time.Duration
	initialCnt  int
	total       int32
	// net.Conn generator
	factory Factory

	// statistics
	getSuccessCnt, getSuccessCost uint64 // in us
	getFailureCnt, getFailureCost uint64 // in us
	returnCnt, closeCnt           uint64
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewBoundedPool returns a new pool based on buffered channels with an initial
// capacity, maximum capacity and waitTimeout to wait for a connection from the pool.
// Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool and total connections is less than the max, a new connection
// will be created via the Factory() method.  Othewise, the call will block until
// a connection is available or the waitTimeout is reached.
func NewBoundedPool(initialCnt, maxCap int, idleTimeout time.Duration, waitTimeout time.Duration, factory Factory) (ConnPool, error) {
	if initialCnt < 0 || maxCap <= 0 || initialCnt > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	pool := &boundedPool{
		closer:      make(chan int),
		conns:       make(chan *pooledConn, maxCap),
		factory:     factory,
		initialCnt:  initialCnt,
		waitTimeout: waitTimeout,
		idleTimeout: idleTimeout,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCnt; i++ {
		conn, err := factory()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		pool.conns <- pool.wrapConn(conn)
		atomic.AddInt32(&pool.total, 1)
	}

	go pool.idleChecker()

	return pool, nil
}

func (pool *boundedPool) checkOnce() {
	now := time.Now()
	for int(atomic.LoadInt32(&pool.total)) > pool.initialCnt && len(pool.conns) > 0 {
		select {
		case conn := <-pool.conns:
			if now.Sub(conn.lastUse) <= pool.idleTimeout {
				select {
				case pool.conns <- conn:
				default:
					// can't be returned, drop
					conn.MarkUnusable()
					conn.Close()
				}
				return
			}
			// drop it
			conn.MarkUnusable()
			conn.Close()
		default:
			return
		}
	}

}

// idleChecker checks the connections in pool and closes those haven't been used for idleTimeout and more than initialCnt.
func (pool *boundedPool) idleChecker() {
	pool.wg.Add(1)
	defer pool.wg.Done()
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			pool.checkOnce()
		case <-pool.closer:
			break LOOP
		}
	}
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (pool *boundedPool) Get() (newConn PooledConn, errOccurred error) {
	conns := pool.conns
	if conns == nil {
		errOccurred = ErrClosed
		return
	}

	// statistics
	begin := time.Now().Nanosecond() / 1e3
	defer func() {
		cost := uint64(time.Now().Nanosecond()/1e3 - begin)
		if errOccurred == nil {
			atomic.AddUint64(&pool.getSuccessCnt, 1)
			atomic.AddUint64(&pool.getSuccessCost, cost)
		} else if errOccurred != ErrClosed {
			atomic.AddUint64(&pool.getFailureCnt, 1)
			atomic.AddUint64(&pool.getFailureCost, cost)
		}
	}()

	// Try and grab a connection from the pool
	select {
	case conn := <-conns:
		if conn == nil {
			errOccurred = ErrClosed
			return
		}
		newConn = conn
		return
	default:
		// Could not get connection, can we create a new one?
		total := atomic.LoadInt32(&pool.total)
		capacity := int32(cap(conns))
		if total < capacity && atomic.AddInt32(&pool.total, 1) <= capacity {
			conn, err := pool.factory()
			if err != nil {
				atomic.AddInt32(&pool.total, -1)
				errOccurred = err
				return
			}
			newConn = pool.wrapConn(conn)
			return
		}
	}

	// The pool was empty and we couldn't create a new one to
	// retry until one is free or we timeout
	select {
	case conn := <-conns:
		if conn == nil {
			errOccurred = ErrClosed
			return
		}
		newConn = conn
		return
	case <-time.After(pool.waitTimeout):
		errOccurred = fmt.Errorf("timed out waiting for free connection")
		return
	}
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (pool *boundedPool) put(conn *pooledConn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if pool.conns == nil {
		// pool is closed, close passed connection
		goto DROP
	}

	// put the resource back into the pool. If the pool is full, drop it
	select {
	case pool.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		goto DROP
	}

DROP:
	conn.MarkUnusable()
	return conn.close()
}

func closeIdleConnWhenShutdown(conn *pooledConn) {
	conn.MarkUnusable()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.close()
}

func (pool *boundedPool) Close() {
	close(pool.closer)
	pool.wg.Wait()
	pool.mu.Lock()
	conns := pool.conns
	pool.conns = nil
	pool.factory = nil
	pool.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for {
		conn := <-conns
		if conn == nil {
			break
		}
		closeIdleConnWhenShutdown(conn)
	}
}

// Len returns current idled connection count in pool
func (pool *boundedPool) Len() int {
	return len(pool.conns)
}

// Total returns total connection count managed by pool
func (pool *boundedPool) Total() int {
	return int(atomic.LoadInt32(&pool.total))
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (pool *boundedPool) wrapConn(conn net.Conn) *pooledConn {
	c := &pooledConn{pool: pool}
	c.Conn = conn
	c.lastUse = time.Now()
	return c
}

func (pool *boundedPool) Statistics() PoolStatistics {
	stat := PoolStatistics{}
	stat.Capacity = cap(pool.conns)
	stat.Idle = len(pool.conns)
	total := atomic.LoadInt32(&pool.total)
	stat.Active = int(total) - stat.Idle

	stat.GetSuccessCnt = atomic.LoadUint64(&pool.getSuccessCnt)
	stat.GetSuccessMillis = atomic.LoadUint64(&pool.getSuccessCost) / 1e3 // us to ms
	stat.GetFailureCnt = atomic.LoadUint64(&pool.getFailureCnt)
	stat.GetFailureMillis = atomic.LoadUint64(&pool.getFailureCost) / 1e3 // us to ms
	stat.ReturnCnt = atomic.LoadUint64(&pool.returnCnt)
	stat.CloseCnt = atomic.LoadUint64(&pool.closeCnt)

	return stat
}

// pooledConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type pooledConn struct {
	net.Conn
	mu       sync.Mutex
	pool     *boundedPool
	lastUse  time.Time
	unusable bool
}

// Close() puts the given connects back to the pool instead of closing it.
func (c *pooledConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.close()
}

// inner close method, no MUTEX lock inside
func (c *pooledConn) close() error {
	if c.Conn == nil {
		return nil
	}
	defer func() {
		c.Conn = nil
	}()

	if c.unusable {
		if c.pool != nil {
			atomic.AddUint64(&c.pool.closeCnt, 1)
		}
		return c.Conn.Close()
	}
	if c.pool != nil {
		atomic.AddUint64(&c.pool.returnCnt, 1)
	}
	return c.pool.put(&pooledConn{
		Conn:     c.Conn,
		pool:     c.pool,
		lastUse:  time.Now(),
		unusable: false,
	})
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (c *pooledConn) MarkUnusable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unusable {
		return
	}
	c.unusable = true
	atomic.AddInt32(&c.pool.total, -1)
}
