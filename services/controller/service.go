//Package ctl provides influxd-ctl service
package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"

	"github.com/angopher/chronus/coordinator"
	"github.com/angopher/chronus/services/migrate"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader   = 4
	MILLISECOND = 1e6
)

type Service struct {
	wg sync.WaitGroup

	Node *influxdb.Node

	MetaClient interface {
		TruncateShardGroups(t time.Time) error
		DeleteDataNode(id uint64) error
		IsDataNodeFreezed(id uint64) bool
		FreezeDataNode(id uint64) error
		UnfreezeDataNode(id uint64) error
		DataNodeByTCPHost(addr string) (*meta.NodeInfo, error)
		RemoveShardOwner(shardID, nodeID uint64) error
		DataNodes() ([]meta.NodeInfo, error)
		RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error)
		Databases() []meta.DatabaseInfo

		ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
		AddShardOwner(shardID, nodeID uint64) error
	}

	TSDBStore interface {
		Path() string
		ShardRelativePath(id uint64) (string, error)
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		DeleteShard(id uint64) error
		Shard(id uint64) *tsdb.Shard
	}

	Listener net.Listener
	Logger   *zap.Logger

	migrateManager *migrate.Manager
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		Logger:         zap.NewNop(),
		migrateManager: migrate.NewManager(c.MaxShardCopyTasks),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting controller service")

	s.wg.Add(1)
	s.migrateManager.Start()
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		if err := s.Listener.Close(); err != nil {
			return err
		}
	}
	s.migrateManager.Close()
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "controller"))
	s.migrateManager.WithLogger(log.With(zap.String("service", "migrate_manager")))
}

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()
		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("Listener closed")
			return
		} else if err != nil {
			s.Logger.Info("Error accepting snapshot request", zap.Error(err))
			continue
		}
		s.Logger.Debug("accept new conn.")

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info(err.Error())
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var typ [1]byte
	_, err := conn.Read(typ[:])
	if err != nil {
		return err
	}

	switch RequestType(typ[0]) {
	case RequestTruncateShard:
		err = s.handleTruncateShard(conn)
		s.truncateShardResponse(conn, err)
	case RequestCopyShard:
		err = s.handleCopyShard(conn)
		s.copyShardResponse(conn, err)
	case RequestCopyShardStatus:
		tasks := s.handleCopyShardStatus(conn)
		s.copyShardStatusResponse(conn, tasks)
	case RequestKillCopyShard:
		err = s.handleKillCopyShard(conn)
		s.killCopyShardResponse(conn, err)
	case RequestRemoveShard:
		err = s.handleRemoveShard(conn)
		s.removeShardResponse(conn, err)
	case RequestRemoveDataNode:
		err = s.handleRemoveDataNode(conn)
		s.removeDataNodeResponse(conn, err)
	case RequestShowDataNodes:
		nodes, err := s.handleShowDataNodes()
		s.showDataNodesResponse(conn, nodes, err)
	case RequestShards:
		groupInfo, err := s.handleShards(conn)
		s.shardsResponse(conn, groupInfo, err)
	case RequestNodeShards:
		shards, err := s.handleNodeShards(conn)
		s.nodeShardsResponse(conn, shards, err)
	case RequestShard:
		db, rp, info, groupInfo, err := s.handleShard(conn)
		s.shardResponse(conn, db, rp, info, groupInfo, err)
	case RequestFreezeDataNode:
		err = s.handleFreezeDataNode(conn)
		s.freezeDataNodeResponse(conn, err)
	}

	return nil
}

func (s *Service) handleTruncateShard(conn net.Conn) error {
	var req TruncateShardRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	t := time.Now().Add(time.Duration(req.DelaySec) * time.Second)
	if err := s.MetaClient.TruncateShardGroups(t); err != nil {
		return err
	}
	return nil
}

func (s *Service) truncateShardResponse(w io.Writer, e error) {
	// Build response.
	var resp TruncateShardResponse
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling truncate shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseTruncateShard), buf); err != nil {
		s.Logger.Error("truncate shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleCopyShard(conn net.Conn) error {
	var req CopyShardRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	return s.copyShard(req.SourceNodeAddr, req.ShardID)
}

func (s *Service) copyShardResponse(w io.Writer, e error) {
	// Build response.
	var resp CommonResp
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseCopyShard), buf); err != nil {
		s.Logger.Error("copy shard WriteTLV fail", zap.Error(err))
	}
}

func toCopyTask(t *migrate.Task) CopyShardTask {
	task := CopyShardTask{}
	task.CurrentSize = t.Copied
	task.Database = t.Database
	task.Rp = t.Retention
	task.ShardID = t.ShardId
	task.Source = t.SrcHost
	return task
}

func (s *Service) writeResponse(w io.Writer, t ResponseType, obj interface{}) {
	// Marshal response to binary.
	buf, err := json.Marshal(obj)
	if err != nil {
		s.Logger.Error(fmt.Sprint("Marshal fail: type=", t, ", error=", err.Error()))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(t), buf); err != nil {
		s.Logger.Error(fmt.Sprint("WriteTLV fail: type=", t, ", error=", err.Error()))
	}
}

func (s *Service) readRequest(conn net.Conn, obj interface{}) error {
	buf, err := coordinator.ReadLV(conn, 10*time.Second)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	if err := json.Unmarshal(buf, obj); err != nil {
		return err
	}
	return nil
}

func (s *Service) handleCopyShardStatus(conn net.Conn) []CopyShardTask {
	tasks := s.migrateManager.Tasks()
	result := make([]CopyShardTask, len(tasks))
	for i, t := range tasks {
		result[i] = toCopyTask(t)
	}
	return result
}

func (s *Service) copyShardStatusResponse(w io.Writer, tasks []CopyShardTask) {
	// Build response.
	var resp CopyShardStatusResponse
	resp.Code = 0
	resp.Msg = "ok"
	resp.Tasks = tasks
	s.writeResponse(w, ResponseCopyShardStatus, &resp)
}

func (s *Service) handleKillCopyShard(conn net.Conn) error {
	var req KillCopyShardRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	s.migrateManager.Kill(req.ShardID, req.SourceNodeAddr)
	return nil
}

func (s *Service) killCopyShardResponse(w io.Writer, e error) {
	// Build response.
	var resp KillCopyShardResponse
	setError(&resp.CommonResp, e)
	s.writeResponse(w, ResponseKillCopyShard, &resp)
}

func (s *Service) handleRemoveShard(conn net.Conn) error {
	s.Logger.Info("handleRemoveShard")
	var req RemoveShardRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	ni, err := s.MetaClient.DataNodeByTCPHost(req.DataNodeAddr)
	if err != nil {
		s.Logger.Error("DataNodeByTCPHost fail.", zap.Error(err))
		return err
	} else if ni == nil {
		err = fmt.Errorf("not find data node by addr:%s", req.DataNodeAddr)
		s.Logger.Error("DataNodeByTCPHost fail.", zap.Error(err))
		return err
	}

	if s.Node.ID == ni.ID {
		shard := s.TSDBStore.Shard(req.ShardID)
		if shard == nil {
			return errors.New("Shard not found")
		}
		if err := s.TSDBStore.DeleteShard(req.ShardID); err != nil {
			s.Logger.Error("DeleteShard fail.", zap.Error(err))
			return err
		}
		if err := s.MetaClient.RemoveShardOwner(req.ShardID, ni.ID); err != nil {
			s.Logger.Error("RemoveShardOwner fail.", zap.Error(err))
			return err
		}
		return nil
	}
	return fmt.Errorf("invalid DataNodeAddr:%s", req.DataNodeAddr)
}

func (s *Service) removeShardResponse(w io.Writer, e error) {
	// Build response.
	var resp RemoveShardResponse
	setError(&resp.CommonResp, e)
	s.writeResponse(w, ResponseRemoveShard, &resp)
}

func (s *Service) handleRemoveDataNode(conn net.Conn) error {
	var req RemoveDataNodeRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	ni, err := s.MetaClient.DataNodeByTCPHost(req.DataNodeAddr)
	if err != nil {
		return err
	} else if ni == nil {
		return fmt.Errorf("not find data node by addr:%s", req.DataNodeAddr)
	}

	return s.MetaClient.DeleteDataNode(ni.ID)
}

func (s *Service) removeDataNodeResponse(w io.Writer, e error) {
	// Build response.
	var resp RemoveDataNodeResponse
	setError(&resp.CommonResp, e)
	s.writeResponse(w, ResponseRemoveDataNode, &resp)
}

func (s *Service) handleFreezeDataNode(conn net.Conn) error {
	var req FreezeDataNodeRequest
	if err := s.readRequest(conn, &req); err != nil {
		return err
	}

	ni, err := s.MetaClient.DataNodeByTCPHost(req.DataNodeAddr)
	if err != nil {
		return err
	} else if ni == nil {
		return fmt.Errorf("not find data node by addr:%s", req.DataNodeAddr)
	}

	if req.Freeze {
		return s.MetaClient.FreezeDataNode(ni.ID)
	} else {
		return s.MetaClient.UnfreezeDataNode(ni.ID)
	}
}

func (s *Service) freezeDataNodeResponse(w io.Writer, e error) {
	// Build response.
	var resp FreezeDataNodeResponse
	setError(&resp.CommonResp, e)
	s.writeResponse(w, ResponseFreezeDataNode, &resp)
}

func (s *Service) handleNodeShards(conn net.Conn) ([]uint64, error) {
	var req GetNodeShardsRequest
	if err := s.readRequest(conn, &req); err != nil {
		return nil, err
	}
	if req.NodeID < 1 {
		return nil, errors.New("Node ID should not be empty")
	}

	shards := make([]uint64, 0)
	dbs := s.MetaClient.Databases()
	for _, db := range dbs {
		infoList := db.ShardInfos()
		for _, info := range infoList {
			if info.OwnedBy(req.NodeID) {
				shards = append(shards, info.ID)
			}
		}
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i] < shards[j]
	})

	return shards, nil
}

func (s *Service) handleShards(conn net.Conn) (*meta.RetentionPolicyInfo, error) {
	var req GetShardsRequest
	if err := s.readRequest(conn, &req); err != nil {
		return nil, err
	}
	if req.Database == "" || req.RetentionPolicy == "" {
		return nil, errors.New("Both database and retention policy should be specified")
	}
	return s.MetaClient.RetentionPolicy(req.Database, req.RetentionPolicy)
}

func fromMetaOwners(owners []meta.ShardOwner) []uint64 {
	nodes := make([]uint64, len(owners))
	for i, owner := range owners {
		nodes[i] = owner.NodeID
	}
	return nodes
}

func fromMetaShards(shards []meta.ShardInfo) []Shard {
	list := make([]Shard, len(shards))
	for i, shard := range shards {
		list[i].ID = shard.ID
		list[i].Nodes = fromMetaOwners(shard.Owners)
	}
	return list
}

func (s *Service) shardsResponse(w io.Writer, rp *meta.RetentionPolicyInfo, e error) {
	// Build response.
	var resp ShardsResponse
	setError(&resp.CommonResp, e)
	if rp != nil {
		resp.Rp = rp.Name
		resp.Duration = int64(rp.Duration / time.Millisecond)
		resp.GroupDuration = int64(rp.ShardGroupDuration / time.Millisecond)
		resp.Replica = rp.ReplicaN
		resp.Groups = make([]ShardGroup, len(rp.ShardGroups))
		for i, g := range rp.ShardGroups {
			resp.Groups[i].ID = g.ID
			resp.Groups[i].StartTime = g.StartTime.UnixNano() / MILLISECOND
			resp.Groups[i].EndTime = g.EndTime.UnixNano() / MILLISECOND
			resp.Groups[i].DeletedAt = g.DeletedAt.UnixNano() / MILLISECOND
			resp.Groups[i].TruncatedAt = g.TruncatedAt.UnixNano() / MILLISECOND
			resp.Groups[i].Shards = fromMetaShards(g.Shards)
		}
	}

	s.writeResponse(w, ResponseShards, &resp)
}

func (s *Service) nodeShardsResponse(w io.Writer, shards []uint64, e error) {
	// Build response.
	var resp NodeShardsResponse
	setError(&resp.CommonResp, e)
	resp.Shards = shards

	s.writeResponse(w, ResponseNodeShards, &resp)
}

func (s *Service) handleShard(conn net.Conn) (string, string, *meta.ShardInfo, *meta.ShardGroupInfo, error) {
	var (
		req       GetShardRequest
		err       error
		db, rp    string
		groupInfo *meta.ShardGroupInfo
	)
	if err = s.readRequest(conn, &req); err != nil {
		goto NOT_FOUND
	}
	if req.ShardID < 1 {
		err = errors.New("ShardID should be specified")
		goto NOT_FOUND
	}
	db, rp, groupInfo = s.MetaClient.ShardOwner(req.ShardID)
	fmt.Println("shardId:", req.ShardID, "=>", db, rp, groupInfo)
	if db == "" || rp == "" || groupInfo == nil {
		err = errors.New("Specified shard could not be found")
		goto NOT_FOUND
	}
	for _, shard := range groupInfo.Shards {
		if shard.ID == req.ShardID {
			return db, rp, &shard, groupInfo, nil
		}
	}
NOT_FOUND:
	return "", "", nil, nil, err
}

func (s *Service) shardResponse(w io.Writer, db, rp string, shard *meta.ShardInfo, groupInfo *meta.ShardGroupInfo, e error) {
	var resp ShardResponse
	setError(&resp.CommonResp, e)
	if e == nil {
		resp.ID = shard.ID
		resp.DB = db
		resp.Rp = rp
		resp.Nodes = fromMetaOwners(shard.Owners)
		resp.GroupID = groupInfo.ID
		resp.Begin = groupInfo.StartTime.UnixNano() / MILLISECOND
		resp.End = groupInfo.EndTime.UnixNano() / MILLISECOND
		resp.Truncated = groupInfo.TruncatedAt.UnixNano() / MILLISECOND
	}
	s.writeResponse(w, ResponseShard, &resp)
}

func (s *Service) handleShowDataNodes() ([]DataNode, error) {
	nodes, err := s.MetaClient.DataNodes()
	if err != nil {
		return nil, err
	}

	var dataNodes []DataNode
	for _, n := range nodes {
		dataNodes = append(dataNodes, DataNode{ID: n.ID, TcpAddr: n.TCPHost, HttpAddr: n.Host, Freezed: s.MetaClient.IsDataNodeFreezed(n.ID)})
	}
	return dataNodes, nil
}

func (s *Service) showDataNodesResponse(w io.Writer, nodes []DataNode, e error) {
	// Build response.
	var resp ShowDataNodesResponse
	setError(&resp.CommonResp, e)
	resp.DataNodes = nodes
	s.writeResponse(w, ResponseShowDataNodes, &resp)
}

func setError(resp *CommonResp, err error) {
	if err != nil {
		resp.Code = 1
		resp.Msg = err.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}
}

type CommonResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type TruncateShardRequest struct {
	DelaySec int64 `json:"delay_sec"`
}
type TruncateShardResponse struct {
	CommonResp
}

type CopyShardRequest struct {
	SourceNodeAddr string `json:"source_node_address"`
	DestNodeAddr   string `json:"dest_node_address"` // is this necessary?
	ShardID        uint64 `json:"shard_id"`
}

type CopyShardResponse struct {
	CommonResp
}

type CopyShardTask struct {
	Database    string `json:"database"`
	Rp          string `json:"retention_policy"`
	ShardID     uint64 `json:"shard_id"`
	TotalSize   uint64 `json:"total_size"` // is this necessary? currently it's ignored.
	CurrentSize uint64 `json:"current_size"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type CopyShardStatusResponse struct {
	CommonResp
	Tasks []CopyShardTask `json:"tasks"`
}

type KillCopyShardRequest struct {
	SourceNodeAddr string `json:"source_node_address"`
	DestNodeAddr   string `json:"dest_node_address"`
	ShardID        uint64 `json:"shard_id"`
}

type KillCopyShardResponse struct {
	CommonResp
}

type GetShardsRequest struct {
	Database, RetentionPolicy string
}

type GetNodeShardsRequest struct {
	NodeID uint64
}

type GetShardRequest struct {
	ShardID uint64
}

type RemoveShardRequest struct {
	DataNodeAddr string `json:"data_node_addr"`
	ShardID      uint64 `json:"shard_id"`
}

type RemoveShardResponse struct {
	CommonResp
}

type RemoveDataNodeRequest struct {
	DataNodeAddr string `json:"data_node_addr"`
}

type RemoveDataNodeResponse struct {
	CommonResp
}

type FreezeDataNodeRequest struct {
	DataNodeAddr string `json:"data_node_addr"`
	Freeze       bool   `json:"freeze"`
}
type FreezeDataNodeResponse struct {
	CommonResp
}

type DataNode struct {
	ID       uint64 `json:"id"`
	TcpAddr  string `json:"tcp_addr"`
	HttpAddr string `json:"http_addr"`
	Freezed  bool   `json:"freezed"`
}

type ShardGroup struct {
	ID          uint64  `json:"id"`
	StartTime   int64   `json:"begin"`
	EndTime     int64   `json:"end"`
	DeletedAt   int64   `json:"deleted_at"`
	TruncatedAt int64   `json:"truncated_at"`
	Shards      []Shard `json:"shards"`
}

type Shard struct {
	ID    uint64   `json:"id"`
	Nodes []uint64 `json:"nodes"`
}

type ShardsResponse struct {
	CommonResp
	Rp            string       `json:"rp"`
	Replica       int          `json:"replica"`
	Duration      int64        `json:"duration"`
	GroupDuration int64        `json:"group_duration"`
	Groups        []ShardGroup `json:"groups"`
}

type NodeShardsResponse struct {
	CommonResp
	Shards []uint64 `json:"shards"`
}

type ShardResponse struct {
	CommonResp
	ID        uint64   `json:"id"`
	DB        string   `json:"db"`
	Rp        string   `json:"rp"`
	Nodes     []uint64 `json:"nodes"`
	GroupID   uint64   `json:"groupId"`
	Begin     int64    `json:"begin"`
	End       int64    `json:"end"`
	Truncated int64    `json:"truncated"`
}

type ShowDataNodesResponse struct {
	CommonResp
	DataNodes []DataNode `json:"data_nodes"`
}

// RequestType indicates the typeof ctl request.
type RequestType byte

const (
	// RequestTruncateShard represents a request for truncating shard.
	_ RequestType = iota
	RequestTruncateShard
	RequestCopyShard
	RequestCopyShardStatus
	RequestKillCopyShard
	RequestRemoveShard
	RequestRemoveDataNode
	RequestShowDataNodes
	RequestShards
	RequestShard
	RequestFreezeDataNode
	RequestNodeShards
)

type ResponseType byte

const (
	_ ResponseType = iota
	ResponseTruncateShard
	ResponseCopyShard
	ResponseCopyShardStatus
	ResponseKillCopyShard
	ResponseRemoveShard
	ResponseRemoveDataNode
	ResponseShowDataNodes
	ResponseShards
	ResponseShard
	ResponseFreezeDataNode
	ResponseNodeShards
)
