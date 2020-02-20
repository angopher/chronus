package raftmeta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	"sync"
	"time"
)

type Linearizabler struct {
	readMu       sync.RWMutex
	readNotifier *notifier
	readwaitc    chan struct{}
	stopping     chan struct{}
	seq          uint64 //identify uniq ReadIndex Request
	timeout      time.Duration
	Node         *RaftNode
}

func NewLinearizabler(node *RaftNode) *Linearizabler {
	return &Linearizabler{
		readNotifier: newNotifier(),
		readwaitc:    make(chan struct{}, 1),
		stopping:     make(chan struct{}),
		seq:          1,
		Node:         node,
		timeout:      time.Second, //TODO:CONFIG
	}
}

func (l *Linearizabler) Stop() {
	close(l.stopping)
}

func (l *Linearizabler) ReadNotify(ctx context.Context) error {
	l.readMu.RLock()
	nc := l.readNotifier
	l.readMu.RUnlock()

	select {
	case l.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Linearizabler) ReadLoop() {
	var rs raft.ReadState
	for {
		leaderChangedNotifier := l.Node.leaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-l.readwaitc:
		case <-l.stopping:
			return
		}

		nextnr := newNotifier()
		l.readMu.Lock()
		nr := l.readNotifier
		l.readNotifier = nextnr
		l.readMu.Unlock()

		rctx := make([]byte, 8)
		seq := l.seq
		l.seq++
		binary.BigEndian.PutUint64(rctx, seq)

		cctx, cancel := context.WithTimeout(context.Background(), l.timeout)
		if err := l.Node.ReadIndex(cctx, rctx); err != nil {
			cancel()
			if err == raft.ErrStopped {
				return
			}

			//fmt.Printf("failed to get read index from raft: %v\n", err)
			//readIndexFailed.Inc()
			nr.notify(err)
			continue
		}
		cancel()

		var (
			isTimeout bool
			done      bool
		)
		for !isTimeout && !done {
			select {
			case rs = <-l.Node.ReadState():
				done = bytes.Equal(rs.RequestCtx, rctx)
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					oldSeq := uint64(0)
					if len(rs.RequestCtx) == 8 {
						oldSeq = binary.BigEndian.Uint64(rs.RequestCtx)
					}

					fmt.Printf("ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader (request ID want %d, got %d)\n", seq, oldSeq)
					//record slowReadIndex
				}
			case <-leaderChangedNotifier:
				isTimeout = true
				//record ReadIndexFail
				nr.notify(errors.New("leader change"))
			case <-time.After(l.timeout):
				//fmt.Println("timed out waiting for read index response (local node might have slow network)")
				nr.notify(errors.New("timeout"))
				isTimeout = true
				//record slowReadIndex
			case <-l.stopping:
				return
			}
		}
		if !done {
			continue
		}

		if ai := l.Node.AppliedIndex(); ai < rs.Index {
			select {
			case <-l.Node.WaitIndex(rs.Index):
			case <-l.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.notify(nil)
	}
}
