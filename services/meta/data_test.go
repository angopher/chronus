package meta_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"

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

func TestCreateDataNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	if err := data.CreateDataNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	expN := meta.NodeInfo{ID: 1, TCPHost: tcpHost, Host: host}
	n := data.DataNode(1)
	if n.ID != expN.ID || n.TCPHost != expN.TCPHost || n.Host != expN.Host {
		t.Fatalf("expected node: %+v, got: %+v", expN, n)
	}

	if err, exp := data.CreateDataNode(host, tcpHost), imeta.ErrNodeExists; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}
}

func TestCreateAndDeleteMetaNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	if err := data.CreateMetaNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	expN := meta.NodeInfo{ID: 1, TCPHost: tcpHost, Host: host}
	n := data.MetaNodes[0]
	if n.ID != expN.ID || n.TCPHost != expN.TCPHost || n.Host != expN.Host {
		t.Fatalf("expected node: %+v, got: %+v", expN, n)
	}

	if err, exp := data.CreateMetaNode(host, tcpHost), imeta.ErrNodeExists; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err, exp := data.DeleteMetaNode(0), imeta.ErrNodeIDRequired; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err, exp := data.DeleteMetaNode(2), imeta.ErrNodeNotFound; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err := data.DeleteMetaNode(expN.ID); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
}

func TestCreateShardGroup(t *testing.T) {
	data := newData()
	name := "testdb"
	policy := meta.DefaultRetentionPolicyName
	data.CreateDatabase(name)
	data.CreateRetentionPolicy(name, meta.DefaultRetentionPolicyInfo(), true)

	if err := data.CreateShardGroup(name, policy, time.Now()); err != imeta.ErrNodeNotFound {
		t.Fatalf("expected err: %s, got: %s", imeta.ErrNodeNotFound, err)
	}

	createTwoDataNode(data)

	if err := data.CreateShardGroup(name, policy, time.Now()); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	rp := data.Database(name).RetentionPolicy(policy)
	sgi := rp.ShardGroups[0]
	if len(sgi.Shards) != 2 {
		t.Fatalf("unexpected 2 shards, got: %d", len(sgi.Shards))
	}

	for _, sh := range sgi.Shards {
		o := sh.Owners[0]
		if len(sh.Owners) != 1 || o.NodeID != 1 && o.NodeID != 2 {
			t.Fatalf("unexpected shard: %+v", sh)
		}
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
	if len(sgi.Shards) != 1 {
		t.Fatalf("unexpected 1 shards, got: %d", len(sgi.Shards))
	}

	owners := sgi.Shards[0].Owners
	if len(owners) != 2 {
		t.Fatalf("unexpected shard: %+v", sgi.Shards[0])
	}

	for _, o := range owners {
		if o.NodeID != 1 && o.NodeID != 2 {
			t.Fatalf("unexpected owner: %+v", o)
		}
	}
}

func TestDeleteDataNode(t *testing.T) {
	data := newData()
	name := "testdb"
	policy := "rp"
	data.CreateDatabase(name)
	createTwoDataNode(data)

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

	data.DeleteDataNode(1)

	rp := data.Database(name).RetentionPolicy("rp")
	sgi := rp.ShardGroups[0]

	owners := sgi.Shards[0].Owners
	if len(owners) != 1 {
		t.Fatalf("unexpected shard: %+v", sgi.Shards[0])
	}

	o := owners[0]
	if o.NodeID != 2 {
		t.Fatalf("unexpected owner: %+v", o)
	}
}

func createTwoDataNode(data *imeta.Data) {
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	data.CreateDataNode(host, tcpHost)

	host = "127.0.0.1:9080"
	tcpHost = "127.0.0.1:9081"
	data.CreateDataNode(host, tcpHost)
}
