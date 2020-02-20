package raftmeta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/angopher/chronus/raftmeta/internal"
	"github.com/angopher/chronus/x"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Transport struct {
	Logger    *zap.Logger
	rwMutex   sync.RWMutex
	PeersAddr map[uint64]string
	Node      interface {
		RecvRaftRPC(ctx context.Context, m raftpb.Message) error
	}
}

func NewTransport() *Transport {
	return &Transport{
		PeersAddr: make(map[uint64]string),
	}
}

func (t *Transport) WithLogger(log *zap.Logger) {
	t.Logger = log.With(zap.String("raftmeta", "Transport"))
}

func (t *Transport) SetPeers(peers map[uint64]string) {
	t.Logger.Info(fmt.Sprintf("SetPeers:%+v", peers))
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()
	t.PeersAddr = peers
}

func (t *Transport) SetPeer(id uint64, addr string) {
	t.Logger.Info("SetPeer", zap.Uint64("ID:", id), zap.String("addr", addr))
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()
	t.PeersAddr[id] = addr
}

func (t *Transport) DeletePeer(id uint64) {
	t.Logger.Info("DeletePeer", zap.Uint64("ID:", id))
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()
	delete(t.PeersAddr, id)
}

func (t *Transport) Peer(id uint64) (string, bool) {
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()
	addr, ok := t.PeersAddr[id]
	return addr, ok
}

func (t *Transport) ClonePeers() map[uint64]string {
	peers := make(map[uint64]string)
	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()
	for k, v := range t.PeersAddr {
		peers[k] = v
	}
	return peers
}

func (t *Transport) SendMessage(messages []raftpb.Message) {
	for _, msg := range messages {
		data, err := msg.Marshal()
		x.Check(err)

		addr, ok := t.Peer(msg.To)
		if !ok {
			t.Logger.Warn("peer not find", zap.Uint64("peer", msg.To))
			continue
		}
		url := fmt.Sprintf("http://%s/message", addr)
		err = Request(url, data)
		if err != nil {
			t.Logger.Error("Request fail:", zap.Error(err), zap.String("url", url))
		}
	}
}

func (t *Transport) RecvMessage(message raftpb.Message) {
	t.Node.RecvRaftRPC(context.Background(), message)
}

func (t *Transport) JoinCluster(ctx *internal.RaftContext, peers []raft.Peer) error {
	x.AssertTrue(len(peers) > 0)
	addr := ""
	for _, p := range peers {
		rc := internal.RaftContext{}
		x.Check(json.Unmarshal(p.Context, &rc))
		addr = rc.Addr
		t.SetPeer(rc.ID, rc.Addr)
	}

	url := fmt.Sprintf("http://%s/update_cluster?op=add", addr)
	data, err := json.Marshal(ctx)
	x.Checkf(err, "encode internal.RaftContext fail")
	return Request(url, data)
}

func (s *RaftNode) HandleUpdateCluster(w http.ResponseWriter, r *http.Request) {
	resp := &CommonResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	var err error
	typ := raftpb.ConfChangeAddNode
	data := []byte{}
	var nodeId uint64
	op := r.FormValue("op")
	if op == "add" || op == "update" {
		data, err = ioutil.ReadAll(r.Body)
		if err != nil {
			resp.RetMsg = err.Error()
			return
		}
		var rc internal.RaftContext
		err = json.Unmarshal(data, &rc)
		x.Check(err)
		nodeId = rc.ID
	} else if op == "remove" {
		typ = raftpb.ConfChangeRemoveNode
		nodeId, err = strconv.ParseUint(r.FormValue("node_id"), 10, 64)
		x.Check(err)
	} else {
		resp.RetMsg = fmt.Sprintf("unkown op:%s", op)
		return
	}

	if nodeId == 0 {
		resp.RetMsg = "invalid node id 0"
		return
	}

	cc := raftpb.ConfChange{
		ID:      s.ID,
		Type:    typ,
		NodeID:  nodeId,
		Context: data,
	}
	err = s.ProposeConfChange(context.Background(), cc)
	if err != nil {
		resp.RetMsg = err.Error()
		return
	}
	resp.RetCode = 0
	resp.RetMsg = "ok"
}

func (s *RaftNode) HandleMessage(w http.ResponseWriter, r *http.Request) {
	resp := &CommonResp{}
	resp.RetCode = 0
	resp.RetMsg = "ok"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	x.Check(err)

	var msg raftpb.Message
	err = msg.Unmarshal(data)
	x.Check(err)
	if msg.Type != raftpb.MsgHeartbeat && msg.Type != raftpb.MsgHeartbeatResp {
		s.Logger.Info("recv message", zap.String("type", msg.Type.String()))
	}
	s.RecvRaftRPC(context.Background(), msg)
}

func Request(url string, data []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				deadline := time.Now().Add(10 * time.Second) //TODO: timeout from config
				c, err := net.DialTimeout(netw, addr, time.Second)
				if err != nil {
					return nil, err
				}
				c.SetDeadline(deadline)
				return c, nil
			},
		},
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	resData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	resp := &CommonResp{RetCode: -1}
	err = json.Unmarshal(resData, resp)
	if resp.RetCode != 0 {
		return fmt.Errorf("fail. err:%s", resp.RetMsg)
	}
	return nil
}
