package meta_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/assert"

	imeta "github.com/angopher/chronus/services/meta"
)

func newData() *imeta.Data {
	return &imeta.Data{
		Data: meta.Data{
			Index:     1,
			ClusterID: 0,
		},
	}
}

func initialTwoDataNodes(data *imeta.Data) (uint64, uint64) {
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	id1, _ := data.CreateDataNode(host, tcpHost)

	host = "127.0.0.1:9080"
	tcpHost = "127.0.0.1:9081"
	id2, _ := data.CreateDataNode(host, tcpHost)

	return id1, id2
}

func TestCreateDataNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"

	// create one node
	id, err := data.CreateDataNode(host, tcpHost)
	assert.Nil(t, err)
	assert.True(t, id > 0)

	// get it by id
	n := data.DataNode(id)
	assert.NotNil(t, n)
	assert.Equal(t, id, n.ID)
	assert.Equal(t, host, n.Host)
	assert.Equal(t, tcpHost, n.TCPHost)

	// refetch, ensure it should not affect inner store
	n.Host = "test"
	n = data.DataNode(id)
	assert.NotNil(t, n)
	assert.Equal(t, id, n.ID)
	assert.Equal(t, host, n.Host)
	assert.Equal(t, tcpHost, n.TCPHost)

	// duplicated creation
	_, err = data.CreateDataNode(host, tcpHost)
	assert.Equal(t, imeta.ErrNodeExists, err)

	// try to delete it (simple deletion)
	err = data.DeleteDataNode(0)
	assert.Equal(t, imeta.ErrNodeIDRequired, err)
	err = data.DeleteDataNode(9999999)
	assert.Equal(t, imeta.ErrNodeNotFound, err)
	err = data.DeleteDataNode(id)
	assert.Nil(t, err)

	// fetch after deletion
	n = data.DataNode(id)
	assert.Nil(t, n)
}

func TestCreateAndDeleteMetaNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"

	// create
	id, err := data.CreateMetaNode(host, tcpHost)
	assert.Nil(t, err)

	// fetch back
	n := data.MetaNode(id)
	assert.NotNil(t, n)
	assert.Equal(t, id, n.ID)
	assert.Equal(t, host, n.Host)
	assert.Equal(t, tcpHost, n.TCPHost)

	// recreation
	_, err = data.CreateMetaNode(host, tcpHost)
	assert.Equal(t, imeta.ErrNodeExists, err)

	// try to delete it (simple deletion)
	err = data.DeleteMetaNode(0)
	assert.Equal(t, imeta.ErrNodeIDRequired, err)
	err = data.DeleteMetaNode(9999999)
	assert.Equal(t, imeta.ErrNodeNotFound, err)
	err = data.DeleteMetaNode(id)
	assert.Nil(t, err)
	err = data.DeleteMetaNode(id)
	assert.Equal(t, imeta.ErrNodeNotFound, err)

	// fetch after deletion
	n = data.MetaNode(id)
	assert.Nil(t, n)
}

func TestCreateShardGroup(t *testing.T) {
	data := newData()
	name := "testdb"
	policy := meta.DefaultRetentionPolicyName
	data.CreateDatabase(name)
	data.CreateRetentionPolicy(name, meta.DefaultRetentionPolicyInfo(), true)

	// create group with no node in cluster
	err := data.CreateShardGroup(name, policy, time.Now())
	assert.Equal(t, imeta.ErrNodeNotFound, err)

	// add nodes to cluster
	id1, id2 := initialTwoDataNodes(data)

	// recreation
	err = data.CreateShardGroup(name, policy, time.Now())
	assert.Nil(t, err)

	rp := data.Database(name).RetentionPolicy(policy)
	assert.NotNil(t, rp)
	// replica = 1(default), shards = node_cnt / replica
	assert.Equal(t, 1, len(rp.ShardGroups))
	assert.Equal(t, 2, len(rp.ShardGroups[0].Shards))
	sgi := rp.ShardGroups[0]

	for _, sh := range sgi.Shards {
		assert.Equal(t, 1, len(sh.Owners))
		o := sh.Owners[0]
		assert.True(t, o.NodeID == id1 || o.NodeID == id2)
	}

	replicaN := 2
	duration := time.Hour
	spec := meta.RetentionPolicySpec{
		Name:               "rp",
		ReplicaN:           &replicaN,
		Duration:           &duration,
		ShardGroupDuration: time.Minute,
	}
	data.CreateRetentionPolicy(name, spec.NewRetentionPolicyInfo(), true)
	data.CreateShardGroup(name, "rp", time.Now())

	rp = data.Database(name).RetentionPolicy("rp")
	sgi = rp.ShardGroups[0]
	assert.NotNil(t, rp)
	assert.Equal(t, 1, len(rp.ShardGroups))
	assert.Equal(t, 1, len(rp.ShardGroups[0].Shards))

	owners := sgi.Shards[0].Owners
	assert.Equal(t, 2, len(owners))

	for _, o := range owners {
		assert.True(t, o.NodeID == id1 || o.NodeID == id2)
	}
}

func TestDeleteDataNode(t *testing.T) {
	data := newData()
	name := "testdb"
	policy := "rp"
	data.CreateDatabase(name)
	id1, id2 := initialTwoDataNodes(data)

	replicaN := 2
	duration := time.Hour
	spec := meta.RetentionPolicySpec{
		Name:               policy,
		ReplicaN:           &replicaN,
		Duration:           &duration,
		ShardGroupDuration: time.Minute,
	}
	data.CreateRetentionPolicy(name, spec.NewRetentionPolicyInfo(), true)
	data.CreateShardGroup(name, policy, time.Now())

	// before deletion, 2 nodes
	rp := data.Database(name).RetentionPolicy("rp")
	assert.NotNil(t, rp)
	assert.Equal(t, 1, len(rp.ShardGroups))
	sgi := rp.ShardGroups[0]
	assert.Equal(t, 1, len(sgi.Shards))
	owners := sgi.Shards[0].Owners
	assert.Equal(t, 2, len(owners))

	data.DeleteDataNode(id1)

	// delete 1 node
	rp = data.Database(name).RetentionPolicy("rp")
	assert.NotNil(t, rp)
	assert.Equal(t, 1, len(rp.ShardGroups))
	sgi = rp.ShardGroups[0]
	assert.Equal(t, 1, len(sgi.Shards))
	owners = sgi.Shards[0].Owners
	assert.Equal(t, 1, len(owners))

	o := owners[0]
	assert.Equal(t, id2, o.NodeID)
}

func TestFreezeDataNode(t *testing.T) {
	data := newData()
	id1, id2 := initialTwoDataNodes(data)
	replicaN := 2
	name := "testdb"
	policy := "rp"
	duration := time.Hour
	data.CreateDatabase(name)
	spec := meta.RetentionPolicySpec{
		Name:               policy,
		ReplicaN:           &replicaN,
		Duration:           &duration,
		ShardGroupDuration: time.Minute,
	}
	data.CreateRetentionPolicy(name, spec.NewRetentionPolicyInfo(), true)
	data.CreateShardGroup(name, policy, time.Now())
	sg, err := data.ShardGroups(name, policy)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(sg))
	assert.Equal(t, 2, len(sg[0].Shards[0].Owners))

	assert.Equal(t, 0, len(data.FreezedDataNodes))

	assert.NotNil(t, data.FreezeDataNode(3333))
	assert.Nil(t, data.FreezeDataNode(id1))
	assert.Nil(t, data.FreezeDataNode(id2))
	assert.Equal(t, []uint64{id1, id2}, data.FreezedDataNodes)
	assert.True(t, data.IsFreezeDataNode(id1))
	assert.True(t, data.IsFreezeDataNode(id2))

	assert.Nil(t, data.UnfreezeDataNode(id2))
	assert.Equal(t, []uint64{id1}, data.FreezedDataNodes)

	assert.Nil(t, data.UnfreezeDataNode(id1))
	assert.Equal(t, []uint64{}, data.FreezedDataNodes)
	assert.Nil(t, data.FreezeDataNode(id1))
	assert.NotNil(t, data.FreezeDataNode(id1))
	assert.Equal(t, []uint64{id1}, data.FreezedDataNodes)
	data.CreateShardGroup(name, policy, time.Now().Add(time.Hour))
	sg, err = data.ShardGroups(name, policy)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(sg))
	assert.Equal(t, 1, len(sg[1].Shards[0].Owners))

	// try delete freezed node
	data.DeleteDataNode(1)
	assert.Equal(t, []uint64{}, data.FreezedDataNodes)
}

func TestClone(t *testing.T) {
	data1 := newData()
	id1, id2 := initialTwoDataNodes(data1)
	data2 := data1.Clone()
	data1.FreezeDataNode(id1)
	data2.FreezeDataNode(id2)
	assert.Equal(t, 1, len(data1.FreezedDataNodes))
	assert.Equal(t, 1, len(data2.FreezedDataNodes))
	assert.NotEqual(t, data1.FreezedDataNodes, data2.FreezedDataNodes)
}
