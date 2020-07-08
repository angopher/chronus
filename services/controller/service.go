//Package ctl provides influxd-ctl service
package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
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
	MuxHeader = 4
)

type Service struct {
	wg sync.WaitGroup

	Node *influxdb.Node

	MetaClient interface {
		TruncateShardGroups(t time.Time) error
		DeleteDataNode(id uint64) error
		DataNodeByTCPHost(addr string) (*meta.NodeInfo, error)
		RemoveShardOwner(shardID, nodeID uint64) error
		DataNodes() ([]meta.NodeInfo, error)

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
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "controller"))
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
		s.Logger.Info("accept new conn.")

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
	}

	return nil
}

func (s *Service) handleTruncateShard(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req TruncateShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
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
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req CopyShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return err
	}

	s.copyShard(req.SourceNodeAddr, req.ShardID)
	return nil
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

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling show copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseCopyShardStatus), buf); err != nil {
		s.Logger.Error("show copy shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleKillCopyShard(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req KillCopyShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return err
	}

	s.migrateManager.Kill(req.ShardID, req.SourceNodeAddr)
	return nil
}

func (s *Service) killCopyShardResponse(w io.Writer, e error) {
	// Build response.
	var resp KillCopyShardResponse
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
		s.Logger.Error("error marshalling show copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseKillCopyShard), buf); err != nil {
		s.Logger.Error("kill copy shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleRemoveShard(conn net.Conn) error {
	s.Logger.Info("handleRemoveShard")
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req RemoveShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		s.Logger.Error("unmarshal fail.", zap.Error(err))
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
		s.Logger.Error("error marshalling remove shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseRemoveShard), buf); err != nil {
		s.Logger.Error("remove shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleRemoveDataNode(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req RemoveDataNodeRequest
	if err := json.Unmarshal(buf, &req); err != nil {
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
		s.Logger.Error("error marshalling remove data node response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseRemoveDataNode), buf); err != nil {
		s.Logger.Error("remove data node WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleShowDataNodes() ([]DataNode, error) {
	nodes, err := s.MetaClient.DataNodes()
	if err != nil {
		return nil, err
	}

	var dataNodes []DataNode
	for _, n := range nodes {
		dataNodes = append(dataNodes, DataNode{ID: n.ID, TcpAddr: n.TCPHost, HttpAddr: n.Host})
	}
	return dataNodes, nil
}

func (s *Service) showDataNodesResponse(w io.Writer, nodes []DataNode, e error) {
	// Build response.
	var resp ShowDataNodesResponse
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}
	resp.DataNodes = nodes

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling show data nodes response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseShowDataNodes), buf); err != nil {
		s.Logger.Error("show data nodes WriteTLV fail", zap.Error(err))
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

type DataNode struct {
	ID       uint64 `json:"id"`
	TcpAddr  string `json:"tcp_addr"`
	HttpAddr string `json:"http_addr"`
}

type ShowDataNodesResponse struct {
	CommonResp
	DataNodes []DataNode `json:"data_nodes"`
}

// RequestType indicates the typeof ctl request.
type RequestType byte

const (
	// RequestTruncateShard represents a request for truncating shard.
	RequestTruncateShard   RequestType = 1
	RequestCopyShard                   = 2
	RequestCopyShardStatus             = 3
	RequestKillCopyShard               = 4
	RequestRemoveShard                 = 5
	RequestRemoveDataNode              = 6
	RequestShowDataNodes               = 7
)

type ResponseType byte

const (
	ResponseTruncateShard   ResponseType = 1
	ResponseCopyShard                    = 2
	ResponseCopyShardStatus              = 3
	ResponseKillCopyShard                = 4
	ResponseRemoveShard                  = 5
	ResponseRemoveDataNode               = 6
	ResponseShowDataNodes                = 7
)
