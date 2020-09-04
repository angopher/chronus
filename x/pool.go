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

	c := &boundedPool{
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
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- c.wrapConn(conn)
		atomic.AddInt32(&c.total, 1)
	}

	go c.idleChecker()

	return c, nil
}

func (c *boundedPool) checkOnce() {
	now := time.Now()
	for int(atomic.LoadInt32(&c.total)) > c.initialCnt && len(c.conns) > 0 {
		select {
		case conn := <-c.conns:
			if now.Sub(conn.lastUse) <= c.idleTimeout {
				select {
				case c.conns <- conn:
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
func (c *boundedPool) idleChecker() {
	c.wg.Add(1)
	defer c.wg.Done()
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			c.checkOnce()
		case <-c.closer:
			break LOOP
		}
	}
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *boundedPool) Get() (PooledConn, error) {
	conns := c.conns
	if conns == nil {
		return nil, ErrClosed
	}

	// Try and grab a connection from the pool
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return conn, nil
	default:
		// Could not get connection, can we create a new one?
		total := atomic.LoadInt32(&c.total)
		capacity := int32(cap(conns))
		if total < capacity && atomic.AddInt32(&c.total, 1) <= capacity {
			conn, err := c.factory()
			if err != nil {
				atomic.AddInt32(&c.total, -1)
				return nil, err
			}

			return c.wrapConn(conn), nil
		}
	}

	// The pool was empty and we couldn't create a new one to
	// retry until one is free or we timeout
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return conn, nil
	case <-time.After(c.waitTimeout):
		return nil, fmt.Errorf("timed out waiting for free connection")
	}

}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *boundedPool) put(conn *pooledConn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		goto DROP
	}

	// put the resource back into the pool. If the pool is full, drop it
	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		goto DROP
	}

DROP:
	conn.MarkUnusable()
	return conn.Close()
}

func (c *boundedPool) Close() {
	close(c.closer)
	c.wg.Wait()
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

// Len returns current idled connection count in pool
func (c *boundedPool) Len() int {
	return len(c.conns)
}

// Total returns total connection count managed by pool
func (c *boundedPool) Total() int {
	return int(atomic.LoadInt32(&c.total))
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *boundedPool) wrapConn(conn net.Conn) *pooledConn {
	p := &pooledConn{c: c}
	p.Conn = conn
	p.lastUse = time.Now()
	return p
}

// pooledConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type pooledConn struct {
	net.Conn
	c        *boundedPool
	lastUse  time.Time
	unusable bool
}

// Close() puts the given connects back to the pool instead of closing it.
func (p *pooledConn) Close() error {
	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	p.lastUse = time.Now()
	return p.c.put(p)
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *pooledConn) MarkUnusable() {
	if p.unusable {
		return
	}
	p.unusable = true
	atomic.AddInt32(&p.c.total, -1)
}
