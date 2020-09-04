package coordinator

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/services/meta"
)

type PoolFactory func(nodeId uint64) (x.ConnPool, error)

type ClientPool struct {
	closer chan int
	wg     sync.WaitGroup

	mu      sync.RWMutex
	pools   map[uint64]*poolEntry
	factory PoolFactory
}

type poolEntry struct {
	pool    x.ConnPool
	lastUse time.Time
}

func NewClientPool(factory PoolFactory) *ClientPool {
	pool := &ClientPool{
		pools:   make(map[uint64]*poolEntry),
		closer:  make(chan int),
		factory: factory,
	}

	go pool.idleCheckLoop()
	return pool
}

func (c *ClientPool) Len() int {
	c.mu.RLock()
	var size int
	for _, p := range c.pools {
		size += p.pool.Len()
	}
	c.mu.RUnlock()
	return size
}

func (c *ClientPool) Total() int {
	c.mu.RLock()
	var size int
	for _, p := range c.pools {
		size += p.pool.Total()
	}
	c.mu.RUnlock()
	return size
}

func (c *ClientPool) idleCheckOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var removed []uint64
	for nodeId, entry := range c.pools {
		if now.Sub(entry.lastUse) <= 600*time.Second {
			continue
		}
		// close it
		removed = append(removed, nodeId)
	}
	for _, id := range removed {
		if entry, ok := c.pools[id]; ok {
			entry.pool.Close()
			delete(c.pools, id)
		}
	}
}

func (c *ClientPool) idleCheckLoop() {
	c.wg.Add(1)
	defer c.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.idleCheckOnce()
		case <-c.closer:
			return
		}
	}
}

func (c *ClientPool) GetConn(nodeID uint64) (x.PooledConn, error) {
	c.mu.RLock()
	if entry, ok := c.pools[nodeID]; ok {
		entry.lastUse = time.Now()
		c.mu.RUnlock()
		return entry.pool.Get()
	}
	c.mu.RUnlock()
	// switch to write lock
	c.mu.Lock()
	defer c.mu.Unlock()
	// create new pool
	pool, err := c.factory(nodeID)
	if err != nil {
		return nil, err
	}
	c.pools[nodeID] = &poolEntry{
		lastUse: time.Now(),
		pool:    pool,
	}
	return pool.Get()
}

func (c *ClientPool) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.pools {
		entry.pool.Close()
	}
	c.pools = nil
}

type ClientConnFactory struct {
	nodeId  uint64
	timeout time.Duration

	metaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
	}
}

func NewClientConnFactory(nodeId uint64, timeout time.Duration, metaClient MetaClient) *ClientConnFactory {
	return &ClientConnFactory{
		nodeId:     nodeId,
		timeout:    timeout,
		metaClient: metaClient,
	}
}

func (c *ClientConnFactory) Dial() (net.Conn, error) {
	ni, err := c.metaClient.DataNode(c.nodeId)
	if err != nil {
		return nil, err
	}

	if ni == nil {
		return nil, fmt.Errorf("node %d does not exist", c.nodeId)
	}

	conn, err := net.DialTimeout("tcp", ni.TCPHost, c.timeout)
	if err != nil {
		return nil, err
	}

	// Write a marker byte for cluster messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
