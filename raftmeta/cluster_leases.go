package raftmeta

import (
	"errors"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

var (
	ErrSkipExpired     = errors.New("Skip an expired lease")
	ErrAcquiredByOther = errors.New("Another node has the lease")
)

// ClusterLeases is a concurrency-safe collection of leases keyed by name.
type ClusterLeases struct {
	mu    sync.Mutex
	m     map[string]*meta.Lease
	d     time.Duration
	sugar *zap.SugaredLogger
}

// NewLeases returns a new instance of Leases.
func NewClusterLeases(d time.Duration, logger *zap.Logger) *ClusterLeases {
	return &ClusterLeases{
		m:     make(map[string]*meta.Lease),
		d:     d,
		sugar: logger.Sugar(),
	}
}

func (leases *ClusterLeases) Get(name string) *meta.Lease {
	leases.mu.Lock()
	defer leases.mu.Unlock()

	return leases.m[name]
}

// CanAcquire returns whether it's necessary actually trying to acquire for now.
func (leases *ClusterLeases) ShouldTry(name string, nodeID uint64) bool {
	leases.mu.Lock()
	defer leases.mu.Unlock()

	now := time.Now()
	l := leases.m[name]
	if l != nil {
		if now.After(l.Expiration) || l.Owner == nodeID {
			return true
		}
		return false
	}

	return true
}

// Acquire acquires a lease with the given name for the given nodeID.
// If the lease doesn't exist or exists but is expired, a valid lease is returned.
// If nodeID already owns the named and unexpired lease, the lease expiration is extended.
// If a different node owns the lease, an error is returned.
func (leases *ClusterLeases) Acquire(name string, nodeID uint64, requestTime int64) (*meta.Lease, error) {
	leases.mu.Lock()
	defer leases.mu.Unlock()

	now := time.Now()
	beginTime := time.Unix(requestTime/1000, (requestTime%1000)*1e6)
	expireTime := beginTime.Add(leases.d)

	if now.After(expireTime) {
		// skip
		leases.sugar.Warnf("Skip expired lock: key=%s, node=%d", name, nodeID)
		return nil, ErrSkipExpired
	}

	l := leases.m[name]
	if l != nil {
		if now.After(l.Expiration) || l.Owner == nodeID {
			l.Expiration = expireTime
			l.Owner = nodeID
			return l, nil
		}
		return l, ErrAcquiredByOther
	}

	l = &meta.Lease{
		Name:       name,
		Expiration: expireTime,
		Owner:      nodeID,
	}

	leases.m[name] = l

	return l, nil
}
