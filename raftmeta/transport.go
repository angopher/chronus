package raftmeta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/angopher/chronus/raftmeta/internal"
	"github.com/angopher/chronus/x"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	loggingLimiter = rate.NewLimiter(1, 1)
	httpClient     = http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
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
		if err != nil && loggingLimiter.Allow() {
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	defer WriteResp(w, &resp)

	var err error
	peers := s.Transport.ClonePeers()
	typ := raftpb.ConfChangeAddNode
	data := []byte{}
	var nodeId uint64
	op := r.FormValue("op")
	switch op {
	case "add", "update":
		data, err = ioutil.ReadAll(r.Body)
		if err != nil {
			resp.RetMsg = err.Error()
			return
		}
		var rc internal.RaftContext
		err = json.Unmarshal(data, &rc)
		x.Check(err)
		nodeId = rc.ID

		// check addr
		for _, addr := range peers {
			if addr == rc.Addr {
				resp.RetMsg = "specified node address already exists"
				return
			}
		}

		// check node id
		switch op {
		case "add":
			if _, ok := peers[nodeId]; ok {
				resp.RetMsg = fmt.Sprintf("specified node id already exists: %d", nodeId)
				return
			}
		case "update":
			if _, ok := peers[nodeId]; !ok {
				resp.RetMsg = fmt.Sprintf("specified node id doesn't exist: %d", nodeId)
				return
			}
		}
	case "remove":
		nodeId, err = strconv.ParseUint(r.FormValue("node_id"), 10, 64)
		if _, ok := peers[nodeId]; !ok {
			resp.RetMsg = fmt.Sprintf("unkown node id: %d", nodeId)
			return
		}
		typ = raftpb.ConfChangeRemoveNode
		x.Check(err)
	default:
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

func (s *RaftNode) HandleStatusNode(w http.ResponseWriter, r *http.Request) {
	resp := &StatusNodeResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	defer WriteResp(w, &resp)

	peers := s.Transport.ClonePeers()
	status := s.Node.Status()
	resp.Status.ID = status.ID
	resp.Status.Vote = status.Vote
	resp.Status.Match = status.Applied
	resp.Status.Next = resp.Status.Match + 1
	resp.Status.Role = strings.Replace(status.RaftState.String(), "State", "", 1)
	if addr, ok := peers[status.ID]; ok {
		resp.Status.Addr = addr
	}
	resp.RetCode = 0
	resp.RetMsg = "ok"
}

func (s *RaftNode) HandleStatusCluster(w http.ResponseWriter, r *http.Request) {
	resp := &StatusClusterResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	defer WriteResp(w, &resp)

	peers := s.Transport.ClonePeers()
	leader := s.Node.Status().Lead
	if leader != s.ID {
		// forward
		if r.URL.Query().Get("forward") == "1" {
			resp.RetMsg = "Error forwarding"
			return
		}
		if addr, ok := peers[leader]; ok {
			r, err := forwardStatusCluster(addr)
			if err != nil {
				resp.RetMsg = err.Error()
			} else {
				resp = r
			}
		}
		return
	}
	nodes := make([]NodeStatus, 0, len(peers))
	status := s.Node.Status()
	prs := status.Progress
	for id, addr := range peers {
		idx := len(nodes)
		nodes = append(nodes, NodeStatus{
			ID:       id,
			Addr:     addr,
			Match:    0,
			Next:     0,
			Role:     "Unreachable",
			Progress: "Unreachable",
		})

		nodeStatus, err := statusPeer(addr)
		if err != nil {
			continue
		}
		nodes[idx].Role = nodeStatus.Role
		nodes[idx].Vote = nodeStatus.Vote

		if pr, ok := prs[id]; ok {
			nodes[idx].Match = pr.Match
			nodes[idx].Next = pr.Next
			nodes[idx].Progress = strings.Replace(pr.State.String(), "ProgressState", "", 1)
		}
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	resp.Nodes = nodes
	resp.Applied = status.Applied
	resp.Commit = status.Commit
	resp.Leader = status.Lead
	resp.Term = status.Term
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
		s.Logger.Debug("recv message", zap.String("type", msg.Type.String()))
	}
	s.RecvRaftRPC(context.Background(), msg)
}

func postRequest(url string, data []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return resData, nil
}

func forwardStatusCluster(addr string) (*StatusClusterResp, error) {
	data, err := postRequest(fmt.Sprint("http://", addr, "/status_cluster?forward=1"), nil)
	if err != nil {
		return nil, err
	}

	resp := &StatusClusterResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	err = json.Unmarshal(data, resp)
	return resp, nil
}

func statusPeer(addr string) (*NodeStatus, error) {
	data, err := postRequest(fmt.Sprint("http://", addr, "/status_node"), nil)
	if err != nil {
		return nil, err
	}

	resp := &StatusNodeResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	err = json.Unmarshal(data, resp)
	if resp.RetCode != 0 {
		return nil, fmt.Errorf("fail. err:%s", resp.RetMsg)
	}
	return &resp.Status, nil
}

func Request(url string, data []byte) error {
	data, err := postRequest(url, data)
	if err != nil {
		return err
	}

	resp := &CommonResp{RetCode: -1}
	err = json.Unmarshal(data, resp)
	if resp.RetCode != 0 {
		return fmt.Errorf("fail. err:%s", resp.RetMsg)
	}
	return nil
}
