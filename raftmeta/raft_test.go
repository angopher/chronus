package raftmeta_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/angopher/chronus/raftmeta"
	"github.com/angopher/chronus/raftmeta/internal"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/angopher/chronus/x"
)

type applyData struct {
	proposal *internal.Proposal
	index    uint64
}

var s1 *raftmeta.MetaService
var s2 *raftmeta.MetaService
var s3 *raftmeta.MetaService
var transports map[uint64]*fakeTransport

func TestMain(t *testing.T) {
	//Initialize transport
	transports = make(map[uint64]*fakeTransport)
	t1 := &fakeTransport{}
	transports[1] = t1
	t2 := &fakeTransport{}
	transports[2] = t2
	t3 := &fakeTransport{}
	transports[3] = t3

	send := func(messages []raftpb.Message) {
		for _, msg := range messages {
			t, _ := transports[msg.To]
			t.RecvMessage(msg)
		}
	}
	t1.SendMessageFn = send
	t2.SendMessageFn = send
	t3.SendMessageFn = send

	t2.JoinClusterFn = func(ctx *internal.RaftContext, peers []raft.Peer) error {
		data, _ := json.Marshal(ctx)
		cc := raftpb.ConfChange{
			ID:      s1.Node.ID,
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  ctx.ID,
			Context: data,
		}
		return s1.Node.ProposeConfChange(context.Background(), cc)
	}
	t3.JoinClusterFn = t2.JoinClusterFn

	//Initialize service1
	s1 = OpenOneService(1, t1, []raftmeta.Peer{})
	s1.InitRouter()
	t1.RecvMessageFn = func(message raftpb.Message) {
		s1.Node.RecvRaftRPC(context.Background(), message)
	}

	//Initialize service2
	s2 = OpenOneService(2, t2, []raftmeta.Peer{
		{Addr: s1.Node.RaftCtx.Addr, RaftId: s1.Node.RaftCtx.ID},
	})
	t2.RecvMessageFn = func(message raftpb.Message) {
		s2.Node.RecvRaftRPC(context.Background(), message)
	}

	//Initialize service3
	s3 = OpenOneService(3, t3, []raftmeta.Peer{
		{Addr: s1.Node.RaftCtx.Addr, RaftId: s1.Node.RaftCtx.ID},
		{Addr: s2.Node.RaftCtx.Addr, RaftId: s2.Node.RaftCtx.ID},
	})
	t3.RecvMessageFn = func(message raftpb.Message) {
		s3.Node.RecvRaftRPC(context.Background(), message)
	}

	go s1.Start()
	go s2.Start()
	go s3.Start()
}

//随机并发创建1000个database
//随机并发创建100w个ShardGroup
//验证状态机的一致性
//验证所有节点的变更日志顺序是否完全一致
func TestNormal(t *testing.T) {
	req := &raftmeta.CreateDatabaseReq{Name: "test"}
	data, _ := json.Marshal(req)
	db := &meta.DatabaseInfo{}
	s1.ProposeAndWait(internal.CreateDatabase, data, db)
}

func OpenOneService(id uint64, t *fakeTransport, peers []raftmeta.Peer) *raftmeta.MetaService {
	c := raftmeta.NewConfig()
	c.RaftId = id
	c.WalDir = fmt.Sprintf("/tmp/.wal%d", id)
	c.MyAddr = fmt.Sprintf("127.0.0.1:%d", 2347+id-1)
	c.Peers = peers
	t.ch = make(chan *applyData, 10000)

	return newService(c, t, func(proposal *internal.Proposal, index uint64) {
		data := &applyData{
			proposal: proposal,
			index:    index,
		}
		t.ch <- data
	})
}

func newService(config raftmeta.Config, t *fakeTransport, cb func(proposal *internal.Proposal, index uint64)) *raftmeta.MetaService {
	metaCli := imeta.NewClient(&meta.Config{
		RetentionAutoCreate: config.RetentionAutoCreate,
		LoggingEnabled:      true,
	})
	err := metaCli.Open()
	x.Check(err)
	log := logger.New(os.Stderr)

	node := raftmeta.NewRaftNode(config, log)
	node.MetaCli = metaCli
	node.ApplyCallBack = cb

	node.Transport = t
	node.InitAndStartNode()
	go node.Run()

	linearRead := raftmeta.NewLinearizabler(node)
	go linearRead.ReadLoop()

	service := raftmeta.NewMetaService(config.MyAddr, metaCli, node, linearRead)
	service.WithLogger(log)
	return service
}

type fakeTransport struct {
	ch            chan *applyData
	SendMessageFn func(messages []raftpb.Message)
	RecvMessageFn func(message raftpb.Message)
	JoinClusterFn func(ctx *internal.RaftContext, peers []raft.Peer) error
}

func (f *fakeTransport) SetPeers(peers map[uint64]string) {
}

func (f *fakeTransport) SetPeer(id uint64, addr string) {
}

func (f *fakeTransport) DeletePeer(id uint64) {
}

func (f *fakeTransport) ClonePeers() map[uint64]string {
	return map[uint64]string{}
}

func (f *fakeTransport) SendMessage(messages []raftpb.Message) {
	f.SendMessageFn(messages)
}

func (f *fakeTransport) RecvMessage(message raftpb.Message) {
	f.RecvMessageFn(message)
}

func (f *fakeTransport) JoinCluster(ctx *internal.RaftContext, peers []raft.Peer) error {
	return f.JoinClusterFn(ctx, peers)
}
