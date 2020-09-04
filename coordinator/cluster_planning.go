package coordinator

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/services/meta"
)

var (
	ErrRetry = errors.New("operation needs another chance")
)

// QueryFn returns ErrRetry indicating operation needs one more try on another node
type QueryFn func(nodeId uint64, shardIds []meta.ShardInfo) (interface{}, error)

// getOwners returns the availible owners filtered by blacklist
func getOwners(blacklist map[uint64]bool, owners []meta.ShardOwner) []meta.ShardOwner {
	if len(blacklist) < 1 {
		return owners
	}
	arr := make([]meta.ShardOwner, 0, len(owners))
	for _, owner := range owners {
		if _, ok := blacklist[owner.NodeID]; ok {
			continue
		}
		arr = append(arr, owner)
	}
	return arr
}

// PlanNodes distributes shards to correct nodes including those local node doesn't have
//	Remote planning is randomized.
//	blacklist is used during retries after queries fail on nodes
func PlanNodes(currentNodeId uint64, shard_queried []meta.ShardInfo, blacklist map[uint64]bool) Node2ShardIDs {
	// shuffle the shards for randomized node selecting
	shards := make([]meta.ShardInfo, len(shard_queried))
	copy(shards, shard_queried)
	rand.Shuffle(len(shards), func(i, j int) {
		shards[i], shards[j] = shards[j], shards[i]
	})
	shardsMap := make(map[uint64][]meta.ShardInfo, x.Min(len(shards), 8))
LOOP:
	for _, shard := range shards {
		// local shard first
		if shard.OwnedBy(currentNodeId) {
			shardsMap[currentNodeId] = append(shardsMap[currentNodeId], shard)
			continue LOOP
		}
		// remote
		owners := getOwners(blacklist, shard.Owners)
		if len(owners) < 1 {
			// no node availible, skip
			continue LOOP
		}
		// node has been selected before has higher priority
		for _, owner := range owners {
			if _, ok := shardsMap[owner.NodeID]; ok {
				shardsMap[owner.NodeID] = append(shardsMap[owner.NodeID], shard)
				continue LOOP
			}
		}
		// random pick
		selectedNodeId := owners[rand.Intn(len(owners))].NodeID
		shardsMap[selectedNodeId] = append(shardsMap[selectedNodeId], shard)
	}

	return shardsMap
}

type Node2ShardIDs map[uint64][]meta.ShardInfo

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

// ExecuteWithRetry executes the query with retry to guarantee all jobs could be executed successfully.
//	errLast holds the last error captured but not means result should be empty if any error occurred
func (me Node2ShardIDs) ExecuteWithRetry(fn QueryFn) (result []interface{}, errLast error) {
	var (
		lock sync.Mutex
		wg   sync.WaitGroup
	)
	wg.Add(len(me))
	for nodeId := range me {
		go func(n uint64, ss []meta.ShardInfo) {
			defer wg.Done()
			arr, err := executePlanWithRetry(n, ss, fn)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errLast = err
			}
			if len(arr) > 0 {
				result = append(result, arr...)
			}
		}(nodeId, me[nodeId])
	}
	wg.Wait()
	return
}

type planSchedule struct {
	NodeID uint64
	Shards []meta.ShardInfo
}

func executePlanWithRetry(nodeId uint64, shards []meta.ShardInfo, fn QueryFn) (result []interface{}, errLast error) {
	var (
		skipNodes      = make(map[uint64]bool)
		arr            []interface{}
		shardsRemained []meta.ShardInfo
		err            error
	)
	plans := []*planSchedule{
		{NodeID: nodeId, Shards: shards},
	}
	for len(plans) > 0 {
		arr, plans, err = executePlans(plans, fn)
		if err != nil {
			errLast = err
		}
		if len(arr) > 0 {
			result = append(result, arr...)
		}
		if len(plans) > 0 {
			// replan
			shardsRemained = shardsRemained[:0]
			for _, p := range plans {
				skipNodes[p.NodeID] = true
				shardsRemained = append(shardsRemained, p.Shards...)
			}
			replanned := PlanNodes(0, shardsRemained, skipNodes)
			plans = plans[:0]
			cnt := 0
			for n, ss := range replanned {
				plans = append(plans, &planSchedule{n, ss})
				cnt += len(ss)
			}
			if cnt < len(shardsRemained) {
				// not all shards can be planned
				fmt.Fprintln(os.Stderr, "Not all shards can be replanned")
			}
		}
	}
	return
}

func executePlans(plans []*planSchedule, fn QueryFn) (result []interface{}, need_retry []*planSchedule, err_last error) {
	for _, plan := range plans {
		obj, err := executePlanSingle(plan.NodeID, plan.Shards, fn)
		if err == nil {
			if obj != nil {
				result = append(result, obj)
			}
			continue
		}
		fmt.Fprintln(os.Stderr, "execute remotely error:", err)
		if err != ErrRetry {
			err_last = err
			continue
		}
		// need retry
		need_retry = append(need_retry, plan)
	}
	return
}

func executePlanSingle(nodeId uint64, shards []meta.ShardInfo, fn QueryFn) (interface{}, error) {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Fprintln(os.Stderr, "panic recover:", r)
		}
	}()
	return fn(nodeId, shards)
}
