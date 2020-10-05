package raftmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/angopher/chronus/raftmeta/internal"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/angopher/chronus/x"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/dgraph/raftwal"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
	"golang.org/x/net/trace"
)

var errInternalRetry = errors.New("Retry Raft proposal internally")

type proposalCtx struct {
	ch      chan error
	ctx     context.Context
	retData interface{}
	err     error
	index   uint64 // RAFT index for the proposal.
}

type proposals struct {
	sync.RWMutex
	all map[string]*proposalCtx
}

func newProposals() *proposals {
	return &proposals{
		all: make(map[string]*proposalCtx),
	}
}

func (p *proposals) Store(key string, pctx *proposalCtx) bool {
	p.Lock()
	defer p.Unlock()
	if _, has := p.all[key]; has {
		return false
	}
	p.all[key] = pctx
	return true
}

func (p *proposals) Delete(key string) {
	if len(key) == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	delete(p.all, key)
}

func (p *proposals) pctx(key string) *proposalCtx {
	p.RLock()
	defer p.RUnlock()
	if pctx := p.all[key]; pctx != nil {
		return pctx
	}
	return new(proposalCtx)
}

func (p *proposals) Done(key string, err error) {
	if len(key) == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	pd, has := p.all[key]
	if !has {
		// If we assert here, there would be a race condition between a context
		// timing out, and a proposal getting applied immediately after. That
		// would cause assert to fail. So, don't assert.
		return
	}
	x.AssertTrue(pd.index != 0)
	if err != nil {
		pd.err = err
	}
	delete(p.all, key)
	pd.ch <- pd.err
}

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.src.Int63()
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	defer r.lk.Unlock()
	r.src.Seed(seed)
}

type Checksum struct {
	needVerify bool //leader use to trigger checksum verify
	index      uint64
	checksum   string
}

type RaftNode struct {
	ID   uint64
	Node raft.Node

	MetaCli MetaClient
	//用于Continuous query
	leases *meta.Leases

	//raft集群内部配置状态
	RaftConfState *raftpb.ConfState
	rwMutex       sync.RWMutex

	//TODO: 这个状态可以消除掉
	RaftCtx *internal.RaftContext

	//存储本地raft节点的配置信息
	RaftConfig *raft.Config

	//来自配置文件的配置信息
	Config Config

	//用于存储raft日志和snapshot
	Storage  *raftwal.DiskStorage
	walStore *badger.DB

	//节点之间的通信模块
	Transport interface {
		SetPeers(peers map[uint64]string)
		SetPeer(id uint64, addr string)
		DeletePeer(id uint64)
		ClonePeers() map[uint64]string
		SendMessage(messages []raftpb.Message)
		JoinCluster(ctx *internal.RaftContext, peers []raft.Peer) error
	}

	Done  chan struct{}
	props *proposals
	rand  *rand.Rand

	applyCh chan *internal.EntryWrapper
	//已经apply的最新index
	appliedIndex uint64

	//最近一次计算的checksum, 用于节点之间对比数据的一致性
	lastChecksum Checksum

	//用于apply日志index监听
	applyWait wait.WaitTime
	// a chan to send out readState
	readStateC chan raft.ReadState

	leaderChanged   chan struct{}
	leaderChangedMu sync.RWMutex

	//only for test
	ApplyCallBack func(proposal *internal.Proposal, index uint64)

	Logger *zap.Logger
}

func NewRaftNode(config Config, logger *zap.Logger) *RaftNode {
	c := &raft.Config{
		ID:              config.RaftId,
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		MaxSizePerMsg:   config.MaxSizePerMsg,
		MaxInflightMsgs: config.MaxInflightMsgs,
		Logger:          newRaftLoggerBridge(logger),
	}

	x.Checkf(os.MkdirAll(config.WalDir, 0700), "Error while creating WAL dir.")
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = config.WalDir
	kvOpt.ValueDir = config.WalDir
	kvOpt.TableLoadingMode = options.MemoryMap
	kvOpt.ValueLogFileSize = 8 << 20
	kvOpt.MaxTableSize = 8 << 20
	kvOpt.NumLevelZeroTables = 2

	walStore, err := badger.Open(kvOpt)
	x.Checkf(err, "Error while creating badger KV WAL store")

	if c.ID == 0 {
		id, err := raftwal.RaftId(walStore)
		x.Check(err)
		c.ID = id
	}

	rc := &internal.RaftContext{
		Addr: config.MyAddr,
		ID:   c.ID,
	}

	//storage := raft.NewMemoryStorage()
	storage := raftwal.Init(walStore, c.ID, 0)
	c.Storage = storage
	return &RaftNode{
		leases:        meta.NewLeases(meta.DefaultLeaseDuration),
		ID:            c.ID,
		RaftConfig:    c,
		Logger:        logger.With(zap.String("raftmeta", "RaftNode")),
		Config:        config,
		RaftCtx:       rc,
		Storage:       storage,
		walStore:      walStore,
		Done:          make(chan struct{}),
		props:         newProposals(),
		rand:          rand.New(&lockedSource{src: rand.NewSource(time.Now().UnixNano())}),
		applyCh:       make(chan *internal.EntryWrapper, config.NumPendingProposals),
		readStateC:    make(chan raft.ReadState, 1),
		applyWait:     wait.NewTimeList(),
		leaderChanged: make(chan struct{}),
		lastChecksum:  Checksum{needVerify: false, index: 0, checksum: ""},
	}
}

func (s *RaftNode) resetPeersInSnapshot(snapshot *raftpb.Snapshot) error {
	var sndata internal.SnapshotData
	err := json.Unmarshal(snapshot.Data, &sndata)
	if err != nil {
		return err
	}
	sndata.PeersAddr = make(map[uint64]string)
	for _, p := range s.Config.Peers {
		sndata.PeersAddr[p.RaftId] = p.Addr
	}
	snapshot.Data, err = json.Marshal(&sndata)
	if err != nil {
		return err
	}
	return nil
}

func (s *RaftNode) Restore(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := os.Lstat(filePath)
	if err != nil {
		return err
	}
	s.Logger.Warn("Restore from snapfile", zap.String("file", filePath), zap.Int64("size", info.Size()))
	snapdata := make([]byte, info.Size())
	_, err = io.ReadFull(f, snapdata)
	if err != nil {
		return err
	}
	snapshot := raftpb.Snapshot{}
	err = json.Unmarshal(snapdata, &snapshot)
	if err != nil {
		return err
	}
	s.Logger.Warn(fmt.Sprintf(
		"Term=%d, Index=%d",
		snapshot.Metadata.Term,
		snapshot.Metadata.Index,
	))
	if s.RaftConfState == nil {
		return errors.New("ConfState has not been set yet")
	}
	snapshot.Metadata.ConfState = *s.RaftConfState
	s.Logger.Warn(fmt.Sprintf(
		"Nodes=%v, Learners=%v",
		snapshot.Metadata.ConfState.Nodes,
		snapshot.Metadata.ConfState.Learners,
	))
	err = s.resetPeersInSnapshot(&snapshot)
	if err != nil {
		return err
	}
	return s.Storage.Save(raftpb.HardState{}, []raftpb.Entry{}, snapshot)
}

func (s *RaftNode) Dump(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	sp, err := s.Storage.Snapshot()
	if err != nil {
		return err
	}
	data, err := json.Marshal(&sp)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}

// uniqueKey is meant to be unique across all the replicas.
func (s *RaftNode) uniqueKey() string {
	return fmt.Sprintf("%02d-%d", s.ID, s.rand.Uint64())
}

func (s *RaftNode) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
	s.applyWait.Trigger(v)
}

func (s *RaftNode) AppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *RaftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	return s.Node.ReadIndex(ctx, rctx)
}

func (s *RaftNode) WaitIndex(index uint64) <-chan struct{} {
	return s.applyWait.Wait(index)
}

func (s *RaftNode) ReadState() <-chan raft.ReadState {
	return s.readStateC
}

func (s *RaftNode) leaderChangedNotify() <-chan struct{} {
	s.leaderChangedMu.RLock()
	s.leaderChangedMu.RUnlock()
	return s.leaderChanged
}

func (s *RaftNode) restoreFromSnapshot() bool {
	s.Logger.Info("restore from snapshot")
	sp, err := s.Storage.Snapshot()
	x.Checkf(err, "Unable to get existing snapshot")

	if raft.IsEmptySnap(sp) {
		s.Logger.Info("empty snapshot. ignore")
		return false
	}
	s.SetConfState(&sp.Metadata.ConfState)
	s.setAppliedIndex(sp.Metadata.Index)

	var sndata internal.SnapshotData
	err = json.Unmarshal(sp.Data, &sndata)
	x.Checkf(err, "internal.SnapshotData UnmarshalBinary fail")

	s.Transport.SetPeers(sndata.PeersAddr)

	metaData := &imeta.Data{}
	err = metaData.UnmarshalBinary(sndata.Data)
	x.Checkf(err, "meta data UnmarshalBinary fail")

	err = s.MetaCli.ReplaceData(metaData)
	x.Checkf(err, "meta cli ReplaceData fail")

	return true
}

func (s *RaftNode) resetPeersFromConfig() {
	for _, peer := range s.Config.Peers {
		s.Transport.SetPeer(peer.RaftId, peer.Addr)
	}
}

func (s *RaftNode) reclaimDiskSpace() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.walStore.RunValueLogGC(0.1)
		case <-s.Done:
			return
		}
	}
}

func (s *RaftNode) InitAndStartNode() {
	peers := make([]raft.Peer, 0, len(s.Config.Peers))
	for _, p := range s.Config.Peers {
		rc := internal.RaftContext{Addr: p.Addr, ID: p.RaftId}
		data, err := json.Marshal(&rc)
		x.Check(err)
		peers = append(peers, raft.Peer{ID: p.RaftId, Context: data})
	}

	idx, restart, err := s.PastLife()
	x.Check(err)
	s.setAppliedIndex(idx)

	if restart {
		s.Logger.Info("Restarting node")
		restored := s.restoreFromSnapshot()
		s.Node = raft.RestartNode(s.RaftConfig)
		if !restored {
			s.resetPeersFromConfig()
		}
	} else {
		s.Logger.Info("Starting node")
		if len(peers) == 0 {
			//创建一个独立的新集群, 成为第一个节点
			data, err := json.Marshal(s.RaftCtx)
			x.Check(err)
			s.Node = raft.StartNode(s.RaftConfig, []raft.Peer{{ID: s.ID, Context: data}})
		} else {
			rpeers := make([]raft.Peer, 0, len(s.Config.Peers))
			for _, peer := range s.Config.Peers {
				rpeers = append(rpeers, raft.Peer{ID: uint64(peer.RaftId)})
			}
			//err := s.joinPeers(peers)
			//x.Checkf(err, "join peers fail")
			//s.Logger.Info("join peers success")
			s.Node = raft.StartNode(s.RaftConfig, rpeers)
			s.resetPeersFromConfig()
		}
	}
}

//请求加入集群
func (s *RaftNode) joinPeers(peers []raft.Peer) error {
	x.AssertTrue(len(peers) > 0)
	return s.Transport.JoinCluster(s.RaftCtx, peers)
}

func (s *RaftNode) Stop() {
	close(s.Done)
	s.Node.Stop()
}

func (s *RaftNode) Run() {
	go s.processApplyCh()

	snapshotTicker := time.NewTicker(time.Duration(s.Config.SnapshotIntervalSec) * time.Second)
	defer snapshotTicker.Stop()

	checkSumTicker := time.NewTicker(time.Duration(s.Config.ChecksumIntervalSec) * time.Second)
	defer checkSumTicker.Stop()

	t := time.NewTicker(time.Duration(s.Config.TickTimeMs) * time.Millisecond)
	defer t.Stop()

	go s.reclaimDiskSpace()

	var leader uint64

	for {
		select {
		case <-snapshotTicker.C:
			if leader == s.ID {
				go func() {
					err := s.trigerSnapshot()
					if err != nil {
						s.Logger.Error("calculateSnapshot fail", zap.Error(err))
					}
				}()
			}
		case <-checkSumTicker.C:
			if leader == s.ID {
				s.triggerChecksum()
			}
		case <-t.C:
			s.Node.Tick()
		case rd := <-s.Node.Ready():
			if len(rd.ReadStates) != 0 {
				select {
				case s.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(time.Second):
					s.Logger.Info("timed out sending read state")
				}
			}

			if rd.SoftState != nil {
				if rd.SoftState.Lead != raft.None && leader != rd.SoftState.Lead {
					//leaderChanges.Inc()
					s.leaderChangedMu.Lock()
					lc := s.leaderChanged
					s.leaderChanged = make(chan struct{})
					close(lc)
					s.leaderChangedMu.Unlock()
					if leader == s.ID {
						s.lastChecksum.needVerify = false
					}
				}
				leader = rd.SoftState.Lead
			}

			if leader == s.ID {
				// Leader can send messages in parallel with writing to disk.
				s.Transport.SendMessage(rd.Messages)
			}

			x.Checkf(s.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot), "terrible! cannot save to storage.")

			if !raft.IsEmptySnap(rd.Snapshot) {
				s.Logger.Info("recv raft snapshot")
				ew := &internal.EntryWrapper{Restore: true}
				s.applyCh <- ew
			}
			for _, entry := range rd.CommittedEntries {
				s.Logger.Debug("process entry", zap.Uint64("term", entry.Term), zap.Uint64("index", entry.Index), zap.String("type", entry.Type.String()))
				ew := &internal.EntryWrapper{Entry: entry, Restore: false}
				s.applyCh <- ew
			}

			if s.ID != leader {
				s.Transport.SendMessage(rd.Messages)
			}

			s.Node.Advance()
		case <-s.Done:
			return
		}
	}
}

//TODO:optimize
func (s *RaftNode) triggerChecksum() {
	s.Logger.Info("trigger checksum")

	if s.lastChecksum.needVerify {
		go func() {
			var verify internal.VerifyChecksum
			verify.Index = s.lastChecksum.index
			verify.Checksum = s.lastChecksum.checksum
			verify.NodeID = s.ID
			data, err := json.Marshal(&verify)
			x.Check(err)

			proposal := &internal.Proposal{
				Type: internal.VerifyChecksumMsg,
				Data: data,
			}
			err = s.ProposeAndWait(context.Background(), proposal, nil)
			if err != nil {
				s.Logger.Error("s.ProposeAndWait fail", zap.Error(err))
			}
		}()
	} else {
		go func() {
			proposal := &internal.Proposal{
				Type: internal.CreateChecksumMsg,
			}
			err := s.ProposeAndWait(context.Background(), proposal, nil)
			if err != nil {
				s.Logger.Error("s.ProposeAndWait fail", zap.Error(err))
			}
		}()
	}
}

func (s *RaftNode) trigerSnapshot() error {
	s.Logger.Info("trigerSnapshot")
	var sn internal.CreateSnapshot
	//_, err = s.Storage.LastIndex()
	//x.Check(err)
	data, err := json.Marshal(sn)
	x.Check(err)
	proposal := &internal.Proposal{
		Type: internal.SnapShot,
		Data: data,
	}

	return s.ProposeAndWait(context.Background(), proposal, nil)
}

func (s *RaftNode) processApplyCh() {
	for {
		select {
		case <-s.Done:
			return
		case ew := <-s.applyCh:
			if ew.Restore {
				s.restoreFromSnapshot()
				continue
			}

			e := ew.Entry
			appliedIndex := s.AppliedIndex()
			if e.Index <= appliedIndex {
				s.Logger.Info("ignored old index", zap.Uint64("index", e.Index), zap.Uint64("applied:", appliedIndex))
				continue
			}
			x.AssertTruef(appliedIndex == e.Index-1, fmt.Sprintf("sync error happend. index:%d, applied:%d", e.Index, appliedIndex))

			if e.Type == raftpb.EntryConfChange {
				s.applyConfChange(&e)
			} else if len(e.Data) == 0 {
				s.Logger.Info("empty entry. ignored")
			} else {
				proposal := &internal.Proposal{}
				if err := json.Unmarshal(e.Data, &proposal); err != nil {
					x.Fatalf("Unable to unmarshal proposal: %v %q\n", err, e.Data)
				}
				err := s.applyCommitted(proposal, e.Index)
				s.Logger.Debug("Applied proposal", zap.String("key", proposal.Key), zap.Uint64("index", e.Index), zap.Error(err))

				s.props.Done(proposal.Key, err)
			}
			s.setAppliedIndex(e.Index)
		}
	}
}

func (s *RaftNode) applyConfChange(e *raftpb.Entry) {
	var cc raftpb.ConfChange
	cc.Unmarshal(e.Data)
	s.Logger.Info(fmt.Sprintf("conf change: %+v", cc))

	if cc.Type == raftpb.ConfChangeRemoveNode {
		s.Transport.DeletePeer(cc.NodeID)
	} else if len(cc.Context) > 0 {
		var rc internal.RaftContext
		x.Check(json.Unmarshal(cc.Context, &rc))
		s.Transport.SetPeer(rc.ID, rc.Addr)
	}
	cs := s.Node.ApplyConfChange(cc)
	s.SetConfState(cs)
}

func (s *RaftNode) SetConfState(cs *raftpb.ConfState) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Info(fmt.Sprintf("Setting conf state to %+v", cs))
	s.RaftConfState = cs
}

func (s *RaftNode) ConfState() *raftpb.ConfState {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.RaftConfState
}

func (s *RaftNode) saveToStorage(h raftpb.HardState, es []raftpb.Entry, sn raftpb.Snapshot) error {
	return s.Storage.Save(h, es, sn)
}

func (s *RaftNode) RecvRaftRPC(ctx context.Context, m raftpb.Message) error {
	return s.Node.Step(ctx, m)
}

func (s *RaftNode) Propose(ctx context.Context, data []byte) error {
	return s.Node.Propose(ctx, data)
}

func (s *RaftNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	return s.Node.ProposeConfChange(ctx, cc)
}

func (s *RaftNode) ProposeAndWait(ctx context.Context, proposal *internal.Proposal, retData interface{}) error {
	if _, ok := ctx.Deadline(); !ok {
		// introduce timeout if needed
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	pctx := &proposalCtx{
		ch:      make(chan error, 1),
		ctx:     ctx,
		retData: retData,
	}
	key := s.uniqueKey()
	x.AssertTruef(s.props.Store(key, pctx), "Found existing proposal with key: [%v]", key)
	defer s.props.Delete(key)

	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("Proposing data with key: %s", key) //TODO: what's the output?
	}

	proposal.Key = key
	data, err := json.Marshal(proposal)
	x.Check(err)

	if err = s.Propose(ctx, data); err != nil {
		return x.Wrapf(err, "While proposing")
	}

	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("Waiting for the proposal.")
	}

	select {
	case err = <-pctx.ch:
		// We arrived here by a call to n.props.Done().
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf("Done with error: %v", err)
		}
		return err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			if tr, ok := trace.FromContext(ctx); ok {
				tr.LazyPrintf("Propose timed out.")
			}
			return errInternalRetry
		}

		return ctx.Err()
	}
}

func (s *RaftNode) PastLife() (idx uint64, restart bool, rerr error) {
	var sp raftpb.Snapshot
	sp, rerr = s.Storage.Snapshot()
	if rerr != nil {
		return
	}
	if !raft.IsEmptySnap(sp) {
		s.Logger.Info(fmt.Sprintf("Found Snapshot, Metadata: %+v", sp.Metadata))
		restart = true
		idx = sp.Metadata.Index
	}

	var hd raftpb.HardState
	hd, rerr = s.Storage.HardState()
	if rerr != nil {
		return
	}
	if !raft.IsEmptyHardState(hd) {
		s.Logger.Info(fmt.Sprintf("Found hardstate: %+v", hd))
		restart = true
	}

	var num int
	num, rerr = s.Storage.NumEntries()
	if rerr != nil {
		return
	}
	s.Logger.Info(fmt.Sprintf("found %d entries", num))
	// We'll always have at least one entry.
	if num > 1 {
		restart = true
	}
	return
}
