package coordinator

import (
	"fmt"
	"net"
	"sort"
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

type StatEntity struct {
	NodeId uint64
	Stat   x.PoolStatistics
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

func (clientPool *ClientPool) Len() int {
	clientPool.mu.RLock()
	var size int
	for _, p := range clientPool.pools {
		size += p.pool.Len()
	}
	clientPool.mu.RUnlock()
	return size
}

func (clientPool *ClientPool) Total() int {
	clientPool.mu.RLock()
	var size int
	for _, p := range clientPool.pools {
		size += p.pool.Total()
	}
	clientPool.mu.RUnlock()
	return size
}

func (clientPool *ClientPool) idleCheckOnce() {
	clientPool.mu.Lock()
	defer clientPool.mu.Unlock()

	now := time.Now()
	var removed []uint64
	for nodeId, entry := range clientPool.pools {
		if now.Sub(entry.lastUse) <= 600*time.Second {
			continue
		}
		// close it
		removed = append(removed, nodeId)
	}
	for _, id := range removed {
		if entry, ok := clientPool.pools[id]; ok {
			entry.pool.Close()
			delete(clientPool.pools, id)
		}
	}
}

func (clientPool *ClientPool) idleCheckLoop() {
	clientPool.wg.Add(1)
	defer clientPool.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			clientPool.idleCheckOnce()
		case <-clientPool.closer:
			return
		}
	}
}

func (clientPool *ClientPool) GetConn(nodeID uint64) (x.PooledConn, error) {
	clientPool.mu.RLock()
	if entry, ok := clientPool.pools[nodeID]; ok {
		entry.lastUse = time.Now()
		clientPool.mu.RUnlock()
		return entry.pool.Get()
	}
	clientPool.mu.RUnlock()
	// switch to write lock
	clientPool.mu.Lock()
	defer clientPool.mu.Unlock()
	// create new pool
	pool, err := clientPool.factory(nodeID)
	if err != nil {
		return nil, err
	}
	clientPool.pools[nodeID] = &poolEntry{
		lastUse: time.Now(),
		pool:    pool,
	}
	return pool.Get()
}

// Dump statistics of connection pools in this client
func (clientPool *ClientPool) Stat() []StatEntity {
	clientPool.mu.RLock()
	defer clientPool.mu.RUnlock()

	stats := make([]StatEntity, 0, len(clientPool.pools))
	for nodeId, pool := range clientPool.pools {
		stats = append(stats, StatEntity{
			Stat:   pool.pool.Statistics(),
			NodeId: nodeId,
		})
	}
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].NodeId < stats[j].NodeId
	})

	return stats
}

func (clientPool *ClientPool) close() {
	clientPool.mu.Lock()
	defer clientPool.mu.Unlock()
	for _, entry := range clientPool.pools {
		entry.pool.Close()
	}
	clientPool.pools = nil
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
