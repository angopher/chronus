package coordinator

import (
	"testing"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/assert"
)

func TestGetOwners(t *testing.T) {
	owners := []meta.ShardOwner{
		{NodeID: 1},
		{NodeID: 2},
		{NodeID: 3},
		{NodeID: 4},
		{NodeID: 5},
		{NodeID: 6},
	}
	ori_len := len(owners)

	assert.Equal(t, ori_len, len(getOwners(nil, owners)))
	assert.Equal(t, ori_len, len(getOwners(map[uint64]bool{}, owners)))
	assert.Equal(t, ori_len, len(getOwners(map[uint64]bool{
		0: true,
	}, owners)))
	assert.Equal(t, ori_len-1, len(getOwners(map[uint64]bool{
		0: true,
		1: true,
	}, owners)))
	assert.Equal(t, ori_len-1, len(getOwners(map[uint64]bool{
		0: true,
		4: true,
	}, owners)))
	assert.Equal(t, ori_len-2, len(getOwners(map[uint64]bool{
		0: true,
		2: true,
		4: true,
	}, owners)))
	assert.Equal(t, 0, len(getOwners(map[uint64]bool{
		0: true,
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
		6: true,
	}, owners)))
}

func verifyPlan(t *testing.T, plan Node2ShardIDs, shards []meta.ShardInfo) bool {
	shardMap := make(map[uint64]meta.ShardInfo, len(shards))
	for _, shard := range shards {
		shardMap[shard.ID] = shard
	}
	for nodeID, ss := range plan {
		for _, s := range ss {
			shard, ok := shardMap[s.ID]
			assert.True(t, ok)
			if !ok {
				return false
			}
			assert.True(t, shard.OwnedBy(nodeID))
			if !shard.OwnedBy(nodeID) {
				return false
			}
			delete(shardMap, s.ID)
		}
	}
	assert.Empty(t, shardMap)
	return true
}

func TestPlanNodes(t *testing.T) {
	shards := []meta.ShardInfo{
		{
			ID: 1,
			Owners: []meta.ShardOwner{
				{NodeID: 1},
				{NodeID: 3},
			},
		}, {
			ID: 2,
			Owners: []meta.ShardOwner{
				{NodeID: 2},
				{NodeID: 4},
			},
		}, {
			ID: 3,
			Owners: []meta.ShardOwner{
				{NodeID: 1},
				{NodeID: 5},
			},
		}, {
			ID: 4,
			Owners: []meta.ShardOwner{
				{NodeID: 2},
				{NodeID: 5},
			},
		},
	}

	result := PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	result = PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	result = PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	result = PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	result = PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	result = PlanNodes(0, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))

	// local node
	resultLocal := PlanNodes(1, shards, nil)
	assert.True(t, verifyPlan(t, resultLocal, shards))
	result = PlanNodes(1, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	assert.Equal(t, len(resultLocal[1]), len(result[1]))
	result = PlanNodes(1, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	assert.Equal(t, len(resultLocal[1]), len(result[1]))
	result = PlanNodes(1, shards, nil)
	assert.True(t, verifyPlan(t, result, shards))
	assert.Equal(t, len(resultLocal[1]), len(result[1]))

	// black list
	result = PlanNodes(0, shards, map[uint64]bool{
		1: true,
	})
	assert.True(t, verifyPlan(t, result, shards))
	assert.Equal(t, 0, len(result[1]))
	result = PlanNodes(0, shards, map[uint64]bool{
		2: true,
	})
	assert.True(t, verifyPlan(t, result, shards))
	assert.Equal(t, 0, len(result[2]))
}
