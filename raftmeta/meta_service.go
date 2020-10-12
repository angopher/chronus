package raftmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/angopher/chronus/raftmeta/internal"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type CommonResp struct {
	RetCode int    `json:"ret_code"`
	RetMsg  string `json:"ret_msg"`
}

type NodeStatus struct {
	ID       uint64 `json:"id"`
	Addr     string `json:"addr"`
	Vote     uint64 `json:"vote"`
	Match    uint64 `json:"match"`
	Next     uint64 `json:"next"`
	Role     string `json:"role"`
	Progress string `json:"progress"`
}

type StatusNodeResp struct {
	CommonResp
	Status NodeStatus `json:"status"`
}

type StatusClusterResp struct {
	CommonResp
	Term    uint64       `json:"term"`
	Commit  uint64       `json:"commit"`
	Applied uint64       `json:"applied"`
	Leader  uint64       `json:"leader"`
	Nodes   []NodeStatus `json:"nodes"`
}

type MetaService struct {
	Logger        *zap.Logger
	Addr          string
	Node          *RaftNode
	cli           *imeta.Client
	Linearizabler interface {
		ReadNotify(ctx context.Context) error
	}
}

func NewMetaService(addr string, cli *imeta.Client, node *RaftNode, l *Linearizabler) *MetaService {
	return &MetaService{
		cli:           cli,
		Addr:          addr,
		Node:          node,
		Linearizabler: l,
	}
}

func (s *MetaService) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("raftmeta", "MetaService"))
}

func (s *MetaService) Stop() {
	s.Node.Stop()
}

func (s *MetaService) InitRouter() {
	http.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		s.Node.HandleMessage(w, r)
	})
	http.HandleFunc("/update_cluster", func(w http.ResponseWriter, r *http.Request) {
		s.Node.HandleUpdateCluster(w, r)
	})
	http.HandleFunc("/status_cluster", func(w http.ResponseWriter, r *http.Request) {
		s.Node.HandleStatusCluster(w, r)
	})
	http.HandleFunc("/status_node", func(w http.ResponseWriter, r *http.Request) {
		s.Node.HandleStatusNode(w, r)
	})

	initHttpHandler(s)
}

func (s *MetaService) Start() {
	ipPort := strings.Split(s.Addr, ":")
	listenAddr := ":" + ipPort[1]
	s.Logger.Info("listen:", zap.String("addr", listenAddr))
	err := http.ListenAndServe(listenAddr, nil)
	if err != nil {
		fmt.Printf("msg=ListenAndServe failed,err=%s\n", err.Error())
	}
}

func (s *MetaService) ProposeAndWait(msgType int, data []byte, retData interface{}) error {
	timeout := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pr := &internal.Proposal{Type: msgType}
	pr.Data = data

	return s.Node.ProposeAndWait(ctx, pr, retData)
}

type CreateDatabaseReq struct {
	Name string
}

type CreateDatabaseResp struct {
	CommonResp
	DbInfo meta.DatabaseInfo
}

func (s *MetaService) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateDatabaseResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDatabase fail", zap.Error(err))
		return
	}

	var req CreateDatabaseReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDatabase fail", zap.Error(err))
		return
	}

	db := &meta.DatabaseInfo{}
	err = s.ProposeAndWait(internal.CreateDatabase, data, db)
	if err != nil {
		resp.RetMsg = fmt.Sprintf("msg=create database failed,database=%s,err_msg=%v", req.Name, err)
		s.Logger.Error("CreateDatabase fail", zap.String("name", req.Name), zap.Error(err))
	}

	resp.DbInfo = *db
	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("CreateDatabase ok", zap.String("name", req.Name))
	return
}

type DropDatabaseReq struct {
	Name string
}

type DropDatabaseResp struct {
	CommonResp
}

func (s *MetaService) DropDatabase(w http.ResponseWriter, r *http.Request) {
	resp := new(DropDatabaseResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropDatabase fail", zap.Error(err))
		return
	}

	var req DropDatabaseReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropDatabase fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropDatabase, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropDatabase fail", zap.String("name", req.Name))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropDatabase ok", zap.String("name", req.Name))
	return
}

type DropRetentionPolicyReq struct {
	Database string
	Policy   string
}

type DropRetentionPolicyResp struct {
	CommonResp
}

func (s *MetaService) DropRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	resp := new(DropRetentionPolicyResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropRetentionPolicy fail", zap.Error(err))
		return
	}

	var req DropRetentionPolicyReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropRetentionPolicy fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropRetentionPolicy, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropRetentionPolicy fail",
			zap.String("database", req.Database),
			zap.String("rp", req.Policy),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropRetentionPolicy ok",
		zap.String("database", req.Database),
		zap.String("rp", req.Policy))
	return
}

type CreateShardGroupReq struct {
	Database  string
	Policy    string
	Timestamp int64
}

type CreateShardGroupResp struct {
	CommonResp
	ShardGroupInfo meta.ShardGroupInfo
}

func (s *MetaService) CreateShardGroup(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateShardGroupResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateShardGroup fail", zap.Error(err))
		return
	}

	var req CreateShardGroupReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateShardGroup fail", zap.Error(err))
		return
	}

	sg := &meta.ShardGroupInfo{}
	err = s.ProposeAndWait(internal.CreateShardGroup, data, sg)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateShardGroup fail",
			zap.String("database", req.Database),
			zap.String("rp", req.Policy),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	resp.ShardGroupInfo = *sg
	s.Logger.Info("CreateShardGroup ok",
		zap.String("database", req.Database),
		zap.String("rp", req.Policy))
	return
}

type CreateDataNodeReq struct {
	HttpAddr string
	TcpAddr  string
}
type CreateDataNodeResp struct {
	CommonResp
	NodeInfo meta.NodeInfo
}

func (s *MetaService) CreateDataNode(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateDataNodeResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDataNode fail", zap.Error(err))
		return
	}

	var req CreateDataNodeReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDataNode fail", zap.Error(err))
		return
	}

	ni := &meta.NodeInfo{}
	err = s.ProposeAndWait(internal.CreateDataNode, data, ni)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDataNode fail",
			zap.Uint64("ID", ni.ID),
			zap.String("Host", ni.Host),
			zap.String("TCPHost", ni.TCPHost),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	resp.NodeInfo = *ni
	s.Logger.Info("CreateDataNode ok",
		zap.Uint64("ID", ni.ID),
		zap.String("Host", ni.Host),
		zap.String("TCPHost", ni.TCPHost))
}

type DeleteDataNodeReq struct {
	Id uint64
}
type DeleteDataNodeResp struct {
	CommonResp
}

func (s *MetaService) DeleteDataNode(w http.ResponseWriter, r *http.Request) {
	resp := new(DeleteDataNodeResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DeleteDataNode fail", zap.Error(err))
		return
	}

	var req DeleteDataNodeReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DeleteDataNode fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DeleteDataNode, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DeleteDataNode fail", zap.Uint64("id", req.Id), zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info(fmt.Sprintf("DeleteDataNode ok, id=%d", req.Id))
}

type RetentionPolicySpec struct {
	Name               string
	ReplicaN           int
	Duration           time.Duration
	ShardGroupDuration time.Duration
}

type CreateRetentionPolicyReq struct {
	Database    string
	Rps         RetentionPolicySpec
	MakeDefault bool
}
type CreateRetentionPolicyResp struct {
	CommonResp
	RetentionPolicyInfo meta.RetentionPolicyInfo
}

func (s *MetaService) CreateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateRetentionPolicyResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateRetentionPolicy fail", zap.Error(err))
		return
	}

	var req CreateRetentionPolicyReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateRetentionPolicy fail", zap.Error(err))
		return
	}

	rpi := &meta.RetentionPolicyInfo{}
	err = s.ProposeAndWait(internal.CreateRetentionPolicy, data, rpi)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateRetentionPolicy fail",
			zap.String("database", req.Database),
			zap.Bool("MakeDefault", req.MakeDefault),
			zap.String("Rps.Name", req.Rps.Name),
			zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
			zap.Duration("Rps.Duration", req.Rps.Duration),
			zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	resp.RetentionPolicyInfo = *rpi
	s.Logger.Info("CreateRetentionPolicy ok",
		zap.String("database", req.Database),
		zap.Bool("MakeDefault", req.MakeDefault),
		zap.String("Rps.Name", req.Rps.Name),
		zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
		zap.Duration("Rps.Duration", req.Rps.Duration),
		zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration))
}

type UpdateRetentionPolicyReq struct {
	Database    string
	Name        string
	Rps         RetentionPolicySpec
	MakeDefault bool
}
type UpdateRetentionPolicyResp struct {
	CommonResp
}

func (s *MetaService) UpdateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	resp := new(UpdateRetentionPolicyResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("UpdateRetentionPolicy fail", zap.Error(err))
		return
	}

	var req UpdateRetentionPolicyReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("UpdateRetentionPolicy fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.UpdateRetentionPolicy, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("UpdateRetentionPolicy fail",
			zap.String("Database", req.Database),
			zap.String("Name", req.Name),
			zap.Bool("MakeDefault", req.MakeDefault),
			zap.String("Rps.Name", req.Rps.Name),
			zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
			zap.Duration("Rps.Duration", req.Rps.Duration),
			zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("UpdateRetentionPolicy ok",
		zap.String("Database", req.Database),
		zap.String("Name", req.Name),
		zap.Bool("MakeDefault", req.MakeDefault),
		zap.String("Rps.Name", req.Rps.Name),
		zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
		zap.Duration("Rps.Duration", req.Rps.Duration),
		zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration))
}

type CreateDatabaseWithRetentionPolicyReq struct {
	Name string
	Rps  RetentionPolicySpec
}
type CreateDatabaseWithRetentionPolicyResp struct {
	CommonResp
	DbInfo meta.DatabaseInfo
}

func (s *MetaService) CreateDatabaseWithRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateDatabaseWithRetentionPolicyResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDatabaseWithRetentionPolicy fail", zap.Error(err))
		return
	}

	var req CreateDatabaseWithRetentionPolicyReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDatabaseWithRetentionPolicy fail", zap.Error(err))
		return
	}

	db := &meta.DatabaseInfo{}
	err = s.ProposeAndWait(internal.CreateDatabaseWithRetentionPolicy, data, db)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateDatabaseWithRetentionPolicy fail",
			zap.String("Name", req.Name),
			zap.String("Rps.Name", req.Rps.Name),
			zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
			zap.Duration("Rps.Duration", req.Rps.Duration),
			zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration),
			zap.Error(err))
		return
	}

	resp.DbInfo = *db
	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("CreateDatabaseWithRetentionPolicy ok",
		zap.String("Name", req.Name),
		zap.String("Rps.Name", req.Rps.Name),
		zap.Int("Rps.ReplicaN", req.Rps.ReplicaN),
		zap.Duration("Rps.Duration", req.Rps.Duration),
		zap.Duration("Rps.ShardGroupDuration", req.Rps.ShardGroupDuration))
}

type CreateUserReq struct {
	Name     string
	Password string
	Admin    bool
}
type CreateUserResp struct {
	CommonResp
	UserInfo meta.UserInfo
}

func (s *MetaService) CreateUser(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateUserResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateUser fail", zap.Error(err))
		return
	}

	var req CreateUserReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateUser fail", zap.Error(err))
		return
	}

	user := &meta.UserInfo{}
	err = s.ProposeAndWait(internal.CreateUser, data, user)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateUser fail",
			zap.String("Name", req.Name),
			zap.String("Password", req.Password),
			zap.Bool("Admin", req.Admin),
			zap.Error(err))
		return
	}

	resp.UserInfo = *user
	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("CreateUser ok",
		zap.String("Name", req.Name),
		zap.String("Password", req.Password),
		zap.Bool("Admin", req.Admin))
}

type DropUserReq struct {
	Name string
}
type DropUserResp struct {
	CommonResp
}

func (s *MetaService) DropUser(w http.ResponseWriter, r *http.Request) {
	resp := new(DropUserResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropUser fail", zap.Error(err))
		return
	}

	var req DropUserReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropUser fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropUser, data, nil)
	if err != nil {
		s.Logger.Error("DropUser fail", zap.String("Name", req.Name), zap.Error(err))
		resp.RetMsg = err.Error()
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropUser ok", zap.String("Name", req.Name))
}

type UpdateUserReq struct {
	Name     string
	Password string
}
type UpdateUserResp struct {
	CommonResp
}

func (s *MetaService) UpdateUser(w http.ResponseWriter, r *http.Request) {
	resp := new(UpdateUserResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("UpdateUser fail", zap.Error(err))
		return
	}

	var req UpdateUserReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("UpdateUser fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.UpdateUser, data, nil)
	if err != nil {
		s.Logger.Error("UpdateUser fail",
			zap.String("Name", req.Name),
			zap.String("Password", req.Password),
			zap.Error(err))
		resp.RetMsg = err.Error()
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("UpdateUser ok",
		zap.String("Name", req.Name),
		zap.String("Password", req.Password))
}

type SetPrivilegeReq struct {
	UserName  string
	Database  string
	Privilege influxql.Privilege
}
type SetPrivilegeResp struct {
	CommonResp
}

func (s *MetaService) SetPrivilege(w http.ResponseWriter, r *http.Request) {
	resp := new(SetPrivilegeResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("SetPrivilege fail", zap.Error(err))
		return
	}

	var req SetPrivilegeReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("SetPrivilege fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.SetPrivilege, data, nil)
	if err != nil {
		s.Logger.Error("SetPrivilege fail",
			zap.String("UserName", req.UserName),
			zap.String("Database", req.Database),
			zap.Error(err))
		resp.RetMsg = err.Error()
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("SetPrivilege ok",
		zap.String("UserName", req.UserName),
		zap.String("Database", req.Database))
}

type SetAdminPrivilegeReq struct {
	UserName string
	Admin    bool
}
type SetAdminPrivilegeResp struct {
	CommonResp
}

func (s *MetaService) SetAdminPrivilege(w http.ResponseWriter, r *http.Request) {
	resp := new(SetAdminPrivilegeResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("SetAdminPrivilege fail", zap.Error(err))
		return
	}

	var req SetAdminPrivilegeReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("SetAdminPrivilege fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.SetAdminPrivilege, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("SetAdminPrivilege fail",
			zap.String("UserName", req.UserName),
			zap.Bool("Admin", req.Admin),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("SetAdminPrivilege ok",
		zap.String("UserName", req.UserName),
		zap.Bool("Admin", req.Admin))
}

type AuthenticateReq struct {
	UserName string
	Password string
}
type AuthenticateResp struct {
	CommonResp
	UserInfo meta.UserInfo
}

func (s *MetaService) Authenticate(w http.ResponseWriter, r *http.Request) {
	resp := new(AuthenticateResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("Authenticate fail", zap.Error(err))
		return
	}

	var req AuthenticateReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("Authenticate fail", zap.Error(err))
		return
	}

	user := &meta.UserInfo{}
	err = s.ProposeAndWait(internal.Authenticate, data, user)
	if err != nil {
		s.Logger.Error("Authenticate fail",
			zap.String("UserName", req.UserName),
			zap.String("Password", req.Password),
			zap.Error(err))
		resp.RetMsg = err.Error()
		return
	}

	resp.UserInfo = *(user)
	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("Authenticate ok",
		zap.String("UserName", req.UserName),
		zap.String("Password", req.Password))
}

type AddShardOwnerReq struct {
	ShardID uint64
	NodeID  uint64
}

type AddShardOwnerResp struct {
	CommonResp
}

func (s *MetaService) AddShardOwner(w http.ResponseWriter, r *http.Request) {
	resp := new(AddShardOwnerResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("AddShardOwner fail", zap.Error(err))
		return
	}

	var req AddShardOwnerReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("AddShardOwner fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.AddShardOwner, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("AddShardOwner fail",
			zap.Uint64("shardID", req.ShardID),
			zap.Uint64("nodeID", req.NodeID),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
}

type RemoveShardOwnerReq struct {
	ShardID uint64
	NodeID  uint64
}

type RemoveShardOwnerResp struct {
	CommonResp
}

func (s *MetaService) RemoveShardOwner(w http.ResponseWriter, r *http.Request) {
	resp := new(RemoveShardOwnerResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("RemoveShardOwner fail", zap.Error(err))
		return
	}

	var req RemoveShardOwnerReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("RemoveShardOwner fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.RemoveShardOwner, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("RemoveShardOwner fail",
			zap.Uint64("shardID", req.ShardID),
			zap.Uint64("nodeID", req.NodeID),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
}

type DropShardReq struct {
	Id uint64
}
type DropShardResp struct {
	CommonResp
}

func (s *MetaService) DropShard(w http.ResponseWriter, r *http.Request) {
	resp := new(DropShardResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropShard fail", zap.Error(err))
		return
	}

	var req DropShardReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropShard fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropShard, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropShard fail",
			zap.Uint64("Id", req.Id),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropShard ok", zap.Uint64("Id", req.Id))
}

type TruncateShardGroupsReq struct {
	Time time.Time
}
type TruncateShardGroupsResp struct {
	CommonResp
}

func (s *MetaService) TruncateShardGroups(w http.ResponseWriter, r *http.Request) {
	resp := new(TruncateShardGroupsResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("TruncateShardGroups fail", zap.Error(err))
		return
	}

	var req TruncateShardGroupsReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("TruncateShardGroups fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.TruncateShardGroups, data, nil)
	if err != nil {
		s.Logger.Error("TruncateShardGroups fail",
			zap.Time("Time", req.Time),
			zap.Error(err))
		resp.RetMsg = err.Error()
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("TruncateShardGroups ok", zap.Time("Time", req.Time))
}

type PruneShardGroupsResp struct {
	CommonResp
}

func (s *MetaService) PruneShardGroups(w http.ResponseWriter, r *http.Request) {
	resp := new(PruneShardGroupsResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	err := s.ProposeAndWait(internal.PruneShardGroups, []byte{}, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("PruneShardGroups fail", zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("PruneShardGroups ok")
}

//DeleteShardGroup
type DeleteShardGroupReq struct {
	Database string
	Policy   string
	Id       uint64
}
type DeleteShardGroupResp struct {
	CommonResp
}

func (s *MetaService) DeleteShardGroup(w http.ResponseWriter, r *http.Request) {
	resp := new(DeleteShardGroupResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("TruncateShardGroups fail", zap.Error(err))
		return
	}

	var req DeleteShardGroupReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("TruncateShardGroups fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DeleteShardGroup, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("TruncateShardGroups fail",
			zap.String("Database", req.Database),
			zap.String("Policy", req.Policy),
			zap.Uint64("Id", req.Id),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("TruncateShardGroups ok",
		zap.String("Database", req.Database),
		zap.String("Policy", req.Policy),
		zap.Uint64("Id", req.Id))
}

//PrecreateShardGroups
type PrecreateShardGroupsReq struct {
	From time.Time
	To   time.Time
}
type PrecreateShardGroupsResp struct {
	CommonResp
}

func (s *MetaService) PrecreateShardGroups(w http.ResponseWriter, r *http.Request) {
	resp := new(PrecreateShardGroupsResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("PrecreateShardGroups fail", zap.Error(err))
		return
	}

	var req PrecreateShardGroupsReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("PrecreateShardGroups fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.PrecreateShardGroups, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("PrecreateShardGroups fail",
			zap.Time("From", req.From),
			zap.Time("To", req.To),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("PrecreateShardGroups ok",
		zap.Time("From", req.From),
		zap.Time("To", req.To))
}

//CreateContinuousQuery
type CreateContinuousQueryReq struct {
	Database string
	Name     string
	Query    string
}
type CreateContinuousQueryResp struct {
	CommonResp
}

func (s *MetaService) CreateContinuousQuery(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateContinuousQueryResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateContinuousQuery fail", zap.Error(err))
		return
	}

	var req CreateContinuousQueryReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateContinuousQuery fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.CreateContinuousQuery, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateContinuousQuery fail",
			zap.String("Database", req.Database),
			zap.String("Name", req.Name),
			zap.String("Query", req.Query),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("CreateContinuousQuery ok",
		zap.String("Database", req.Database),
		zap.String("Name", req.Name),
		zap.String("Query", req.Query))
}

//DropContinuousQuery
type DropContinuousQueryReq struct {
	Database string
	Name     string
}
type DropContinuousQueryResp struct {
	CommonResp
}

func (s *MetaService) DropContinuousQuery(w http.ResponseWriter, r *http.Request) {
	resp := new(DropContinuousQueryResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropContinuousQuery fail", zap.Error(err))
		return
	}

	var req DropContinuousQueryReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropContinuousQuery fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropContinuousQuery, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropContinuousQuery fail",
			zap.String("Database", req.Database),
			zap.String("Name", req.Name),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropContinuousQuery ok",
		zap.String("Database", req.Database),
		zap.String("Name", req.Name))
}

//CreateSubscription
type CreateSubscriptionReq struct {
	Database     string
	Rp           string
	Name         string
	Mode         string
	Destinations []string
}
type CreateSubscriptionResp struct {
	CommonResp
}

func (s *MetaService) CreateSubscription(w http.ResponseWriter, r *http.Request) {
	resp := new(CreateSubscriptionResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateSubscriptionReq fail", zap.Error(err))
		return
	}

	var req CreateSubscriptionReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateSubscriptionReq fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.CreateSubscription, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("CreateSubscriptionReq fail",
			zap.String("Database", req.Database),
			zap.String("Rp", req.Rp),
			zap.String("Name", req.Name),
			zap.String("Mode", req.Mode),
			zap.Strings("Destinations", req.Destinations),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("CreateSubscriptionReq ok",
		zap.String("Database", req.Database),
		zap.String("Rp", req.Rp),
		zap.String("Name", req.Name),
		zap.String("Mode", req.Mode),
		zap.Strings("Destinations", req.Destinations))
}

//DropSubscription
type DropSubscriptionReq struct {
	Database string
	Rp       string
	Name     string
}
type DropSubscriptionResp struct {
	CommonResp
}

func (s *MetaService) DropSubscription(w http.ResponseWriter, r *http.Request) {
	resp := new(DropSubscriptionResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropSubscription fail", zap.Error(err))
		return
	}

	var req DropSubscriptionReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropSubscription fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.DropSubscription, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("DropSubscription fail",
			zap.String("Database", req.Database),
			zap.String("Rp", req.Rp),
			zap.String("Name", req.Name),
			zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info("DropSubscription ok",
		zap.String("Database", req.Database),
		zap.String("Rp", req.Rp),
		zap.String("Name", req.Name))
}

type AcquireLeaseReq struct {
	Name   string
	NodeId uint64
}

type AcquireLeaseResp struct {
	CommonResp
	Lease meta.Lease
}

func (s *MetaService) AcquireLease(w http.ResponseWriter, r *http.Request) {
	resp := new(AcquireLeaseResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("AcquireLease fail", zap.Error(err))
		return
	}

	var req AcquireLeaseReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("AcquireLease fail", zap.Error(err))
		return
	}

	lease := &meta.Lease{}
	err = s.ProposeAndWait(internal.AcquireLease, data, lease)
	if err != nil {
		resp.RetMsg = err.Error()
		if !strings.Contains(err.Error(), "another node has the lease") {
			s.Logger.Error("AcquireLease fail",
				zap.String("Name", req.Name),
				zap.Uint64("NodeId", req.NodeId),
				zap.Error(err))
		}
		return
	}

	resp.Lease = *lease
	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Debug("AcquireLease ok",
		zap.String("Name", req.Name),
		zap.Uint64("NodeId", req.NodeId))
}

type DataResp struct {
	CommonResp
	Data []byte
}

func (s *MetaService) Data(w http.ResponseWriter, r *http.Request) {
	resp := new(DataResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.Linearizabler.ReadNotify(ctx)
	if err != nil {
		resp.RetMsg = err.Error()
		return
	}

	data := s.cli.Data()
	resp.Data, err = data.MarshalBinary()
	if err != nil {
		resp.RetMsg = err.Error()
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
}

type FreezeDataNodeReq struct {
	Id     uint64
	Freeze bool
}
type FreezeDataNodeResp struct {
	CommonResp
}

func (s *MetaService) FreezeDataNode(w http.ResponseWriter, r *http.Request) {
	resp := new(FreezeDataNodeResp)
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("FreezeDataNode fail", zap.Error(err))
		return
	}

	var req FreezeDataNodeReq
	if err := json.Unmarshal(data, &req); err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error("FreezeDataNode fail", zap.Error(err))
		return
	}

	err = s.ProposeAndWait(internal.FreezeDataNode, data, nil)
	if err != nil {
		resp.RetMsg = err.Error()
		s.Logger.Error(fmt.Sprintf("FreezeDataNode fail, id=%d, freeze=%t", req.Id, req.Freeze), zap.Error(err))
		return
	}

	resp.RetCode = 0
	resp.RetMsg = "ok"
	s.Logger.Info(fmt.Sprintf("FreezeDataNode ok, id=%d, freeze=%t", req.Id, req.Freeze))
}

type PingResp struct {
	CommonResp
	Index uint64
}

func (s *MetaService) Ping(w http.ResponseWriter, r *http.Request) {
	resp := new(PingResp)
	resp.Index = s.cli.DataIndex()
	resp.RetCode = 0
	resp.RetMsg = "ok"
	WriteResp(w, &resp)
}

func WriteResp(w http.ResponseWriter, v interface{}) error {
	bytes, _ := json.Marshal(v)
	_, err := w.Write(bytes)
	if err != nil {
		return err
	}

	return nil

}

func initHttpHandler(s *MetaService) {
	http.HandleFunc(DATA_PATH, s.Data)
	http.HandleFunc(CREATE_DATABASE_PATH, s.CreateDatabase)
	http.HandleFunc(DROP_DATABASE_PATH, s.DropDatabase)
	http.HandleFunc(CREATE_SHARD_GROUP_PATH, s.CreateShardGroup)
	http.HandleFunc(CREATE_DATA_NODE_PATH, s.CreateDataNode)

	http.HandleFunc(DROP_RETENTION_POLICY_PATH, s.DropRetentionPolicy)
	http.HandleFunc(DELETE_DATA_NODE_PATH, s.DeleteDataNode)
	http.HandleFunc(FREEZE_DATA_NODE_PATH, s.FreezeDataNode)
	http.HandleFunc(CREATE_RETENTION_POLICY_PATH, s.CreateRetentionPolicy)
	http.HandleFunc(UPDATE_RETENTION_POLICY_PATH, s.UpdateRetentionPolicy)
	http.HandleFunc(CREATE_USER_PATH, s.CreateUser)
	http.HandleFunc(DROP_USER_PATH, s.DropUser)
	http.HandleFunc(UPDATE_USER_PATH, s.UpdateUser)
	http.HandleFunc(SET_PRIVILEGE_PATH, s.SetPrivilege)
	http.HandleFunc(SET_ADMIN_PRIVILEGE, s.SetAdminPrivilege)
	http.HandleFunc(AUTHENTICATE_PATH, s.Authenticate)
	http.HandleFunc(DROP_SHARD_PATH, s.DropShard)
	http.HandleFunc(ADD_SHARD_OWNER, s.AddShardOwner)
	http.HandleFunc(REMOVE_SHARD_OWNER, s.RemoveShardOwner)
	http.HandleFunc(TRUNCATE_SHARD_GROUPS_PATH, s.TruncateShardGroups)
	http.HandleFunc(PRUNE_SHARD_GROUPS_PATH, s.PruneShardGroups)
	http.HandleFunc(DELETE_SHARD_GROUP_PATH, s.DeleteShardGroup)
	http.HandleFunc(PRECREATE_SHARD_GROUPS_PATH, s.PrecreateShardGroups)
	http.HandleFunc(CREATE_DATABASE_WITH_RETENTION_POLICY_PATH, s.CreateDatabaseWithRetentionPolicy)
	http.HandleFunc(CREATE_CONTINUOUS_QUERY_PATH, s.CreateContinuousQuery)
	http.HandleFunc(DROP_CONTINUOUS_QUERY_PATH, s.DropContinuousQuery)
	http.HandleFunc(CREATE_SUBSCRIPTION_PATH, s.CreateSubscription)
	http.HandleFunc(DROP_SUBSCRIPTION_PATH, s.DropSubscription)
	http.HandleFunc(ACQUIRE_LEASE_PATH, s.AcquireLease)
	http.HandleFunc(PING_PATH, s.Ping)
}
