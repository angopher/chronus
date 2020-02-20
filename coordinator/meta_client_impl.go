package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"

	"github.com/angopher/chronus/raftmeta"
	imeta "github.com/angopher/chronus/services/meta"
)

type MetaClientImpl struct {
	Addrs []string
}

//	return &MetaClientImpl{MetaServiceHost: "127.0.0.1:1234"}

func (me *MetaClientImpl) Url(path string) string {
	return fmt.Sprintf("http://%s%s", me.Addrs[rand.Intn(len(me.Addrs))], path)
}

func (me *MetaClientImpl) AcquireLease(NodeID uint64, name string) (*meta.Lease, error) {
	req := raftmeta.AcquireLeaseReq{Name: name, NodeId: NodeID}
	var resp raftmeta.AcquireLeaseResp
	err := RequestAndParseResponse(me.Url(raftmeta.ACQUIRE_LEASE_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	l := &meta.Lease{}
	*l = resp.Lease
	return l, nil
}

func (me *MetaClientImpl) ClusterID() uint64 {
	//TODO:
	return 0
}

func (me *MetaClientImpl) Data() (*imeta.Data, error) {
	var resp raftmeta.DataResp
	err := RequestAndParseResponse(me.Url(raftmeta.DATA_PATH), "", &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	data := new(imeta.Data)
	err = data.UnmarshalBinary(resp.Data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (me *MetaClientImpl) Ping() (uint64, error) {
	var resp raftmeta.PingResp
	err := RequestAndParseResponse(me.Url(raftmeta.PING_PATH), "", &resp)
	if err != nil {
		return 0, err
	}

	if resp.RetCode != 0 {
		return 0, errors.New(resp.RetMsg)
	}

	return resp.Index, nil
}

func (me *MetaClientImpl) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	req := raftmeta.CreateDatabaseReq{Name: name}
	var resp raftmeta.CreateDatabaseResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_DATABASE_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	db := new(meta.DatabaseInfo)
	*db = resp.DbInfo
	return db, nil
}

func (me *MetaClientImpl) DeleteDataNode(id uint64) error {
	req := raftmeta.DeleteDataNodeReq{Id: id}
	var resp raftmeta.DeleteDataNodeResp
	err := RequestAndParseResponse(me.Url(raftmeta.DELETE_DATA_NODE_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) AddShardOwner(shardID, nodeID uint64) error {
	req := raftmeta.AddShardOwnerReq{
		ShardID: shardID,
		NodeID:  nodeID,
	}

	var resp raftmeta.AddShardOwnerResp
	err := RequestAndParseResponse(me.Url(raftmeta.ADD_SHARD_OWNER), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) RemoveShardOwner(shardID, nodeID uint64) error {
	req := raftmeta.RemoveShardOwnerReq{
		ShardID: shardID,
		NodeID:  nodeID,
	}

	var resp raftmeta.RemoveShardOwnerResp
	err := RequestAndParseResponse(me.Url(raftmeta.REMOVE_SHARD_OWNER), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	req := raftmeta.CreateShardGroupReq{
		Database:  database,
		Policy:    policy,
		Timestamp: timestamp.Unix(),
	}

	var resp raftmeta.CreateShardGroupResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_SHARD_GROUP_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	sg := new(meta.ShardGroupInfo)
	*sg = resp.ShardGroupInfo
	return sg, nil
}

func (me *MetaClientImpl) CreateDataNode(httpAddr, tcpAddr string) (*meta.NodeInfo, error) {
	req := raftmeta.CreateDataNodeReq{
		HttpAddr: httpAddr,
		TcpAddr:  tcpAddr,
	}

	var resp raftmeta.CreateDataNodeResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_DATA_NODE_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	ni := new(meta.NodeInfo)
	*ni = resp.NodeInfo
	return ni, nil
}

func (me *MetaClientImpl) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	replica := 1
	if spec.ReplicaN != nil {
		replica = *spec.ReplicaN
	}

	duration := time.Duration(0)
	if spec.Duration != nil {
		duration = *spec.Duration
	}

	req := raftmeta.CreateDatabaseWithRetentionPolicyReq{
		Name: name,
		Rps: raftmeta.RetentionPolicySpec{
			Name:               spec.Name,
			ReplicaN:           replica,
			Duration:           duration,
			ShardGroupDuration: spec.ShardGroupDuration,
		},
	}
	var resp raftmeta.CreateDatabaseWithRetentionPolicyResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_DATABASE_WITH_RETENTION_POLICY_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	db := new(meta.DatabaseInfo)
	*db = resp.DbInfo
	return db, nil
}

func (me *MetaClientImpl) CreateContinuousQuery(database, name, query string) error {
	req := raftmeta.CreateContinuousQueryReq{Database: database, Name: name, Query: query}
	var resp raftmeta.CreateContinuousQueryResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_CONTINUOUS_QUERY_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	replica := 1
	if spec.ReplicaN != nil {
		replica = *spec.ReplicaN
	}

	duration := time.Duration(0)
	if spec.Duration != nil {
		duration = *spec.Duration
	}

	req := raftmeta.CreateRetentionPolicyReq{
		Database: database,
		Rps: raftmeta.RetentionPolicySpec{
			Name:               spec.Name,
			ReplicaN:           replica,
			Duration:           duration,
			ShardGroupDuration: spec.ShardGroupDuration,
		},
		MakeDefault: makeDefault,
	}
	var resp raftmeta.CreateRetentionPolicyResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_RETENTION_POLICY_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	sgi := new(meta.RetentionPolicyInfo)
	*sgi = resp.RetentionPolicyInfo
	return sgi, nil
}

func (me *MetaClientImpl) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	req := raftmeta.CreateSubscriptionReq{
		Database:     database,
		Rp:           rp,
		Name:         name,
		Mode:         mode,
		Destinations: destinations,
	}
	var resp raftmeta.CreateSubscriptionResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_SUBSCRIPTION_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) CreateUser(name, password string, admin bool) (meta.User, error) {
	req := raftmeta.CreateUserReq{Name: name, Password: password, Admin: admin}
	var resp raftmeta.CreateUserResp
	err := RequestAndParseResponse(me.Url(raftmeta.CREATE_USER_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	user := new(meta.UserInfo)
	*user = resp.UserInfo
	return user, nil
}

func (me *MetaClientImpl) DropShard(id uint64) error {
	req := raftmeta.DropShardReq{Id: id}
	var resp raftmeta.DropShardResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_SHARD_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) DropContinuousQuery(database, name string) error {
	req := raftmeta.DropContinuousQueryReq{Database: database, Name: name}
	var resp raftmeta.DropContinuousQueryResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_CONTINUOUS_QUERY_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) DropDatabase(name string) error {
	req := raftmeta.DropDatabaseReq{Name: name}
	var resp raftmeta.DropDatabaseResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_DATABASE_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) DropRetentionPolicy(database, name string) error {
	req := raftmeta.DropRetentionPolicyReq{Database: database, Policy: name}
	var resp raftmeta.DropRetentionPolicyResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_RETENTION_POLICY_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) DropSubscription(database, rp, name string) error {
	req := raftmeta.DropSubscriptionReq{Database: database, Rp: rp, Name: name}
	var resp raftmeta.DropSubscriptionResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_SUBSCRIPTION_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) DropUser(name string) error {
	req := raftmeta.DropUserReq{Name: name}
	var resp raftmeta.DropUserResp
	err := RequestAndParseResponse(me.Url(raftmeta.DROP_USER_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) SetAdminPrivilege(username string, admin bool) error {
	req := raftmeta.SetAdminPrivilegeReq{UserName: username, Admin: admin}
	var resp raftmeta.SetAdminPrivilegeResp
	err := RequestAndParseResponse(me.Url(raftmeta.SET_ADMIN_PRIVILEGE), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) SetPrivilege(username, database string, p influxql.Privilege) error {
	req := raftmeta.SetPrivilegeReq{UserName: username, Database: database, Privilege: p}
	var resp raftmeta.SetPrivilegeResp
	err := RequestAndParseResponse(me.Url(raftmeta.SET_PRIVILEGE_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) TruncateShardGroups(t time.Time) error {
	req := raftmeta.TruncateShardGroupsReq{Time: t}
	var resp raftmeta.TruncateShardGroupsResp
	err := RequestAndParseResponse(me.Url(raftmeta.TRUNCATE_SHARD_GROUPS_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) DeleteShardGroup(database, policy string, id uint64) error {
	req := raftmeta.DeleteShardGroupReq{Database: database, Policy: policy, Id: id}
	var resp raftmeta.DeleteShardGroupResp
	err := RequestAndParseResponse(me.Url(raftmeta.DELETE_SHARD_GROUP_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) PruneShardGroups() error {
	var resp raftmeta.TruncateShardGroupsResp
	err := RequestAndParseResponse(me.Url(raftmeta.PRUNE_SHARD_GROUPS_PATH), "", &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) PrecreateShardGroups(from, to time.Time) error {
	req := raftmeta.PrecreateShardGroupsReq{From: from, To: to}
	var resp raftmeta.PrecreateShardGroupsResp
	err := RequestAndParseResponse(me.Url(raftmeta.PRECREATE_SHARD_GROUPS_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}
	return nil
}

func (me *MetaClientImpl) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	replica := 1
	if rpu.ReplicaN != nil {
		replica = *rpu.ReplicaN
	}

	duration := time.Duration(0)
	if rpu.Duration != nil {
		duration = *rpu.Duration
	}

	sduration := time.Duration(0)
	if rpu.ShardGroupDuration != nil {
		sduration = *rpu.ShardGroupDuration
	}

	rpName := ""
	if rpu.Name != nil {
		rpName = *rpu.Name
	}

	req := raftmeta.UpdateRetentionPolicyReq{
		Database: database,
		Name:     name,
		Rps: raftmeta.RetentionPolicySpec{
			Name:               rpName,
			ReplicaN:           replica,
			Duration:           duration,
			ShardGroupDuration: sduration,
		},
	}
	var resp raftmeta.UpdateRetentionPolicyResp
	err := RequestAndParseResponse(me.Url(raftmeta.UPDATE_RETENTION_POLICY_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) UpdateUser(name, password string) error {
	req := raftmeta.UpdateUserReq{Name: name, Password: password}
	var resp raftmeta.UpdateUserResp
	err := RequestAndParseResponse(me.Url(raftmeta.UPDATE_USER_PATH), &req, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}

func (me *MetaClientImpl) Authenticate(username, password string) (meta.User, error) {
	req := raftmeta.AuthenticateReq{UserName: username, Password: password}
	var resp raftmeta.AuthenticateResp
	err := RequestAndParseResponse(me.Url(raftmeta.AUTHENTICATE_PATH), &req, &resp)
	if err != nil {
		return nil, err
	}

	if resp.RetCode != 0 {
		return nil, errors.New(resp.RetMsg)
	}

	user := new(meta.UserInfo)
	*user = resp.UserInfo
	return user, nil
}

func RequestAndParseResponse(url string, data interface{}, resp interface{}) error {
	reqBody, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBody))
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

	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(resBody, resp)
}
