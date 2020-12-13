package coordinator

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	imeta "github.com/angopher/chronus/services/meta"
)

type ClusterMetaClient struct {
	NodeID         uint64
	cache          *imeta.Client
	metaCli        *MetaClientImpl
	pingIntervalMs int64
	Logger         *zap.Logger
}

func NewMetaClient(mc *meta.Config, cc Config, nodeID uint64) *ClusterMetaClient {
	return &ClusterMetaClient{
		NodeID: nodeID,
		metaCli: &MetaClientImpl{
			Addrs: cc.MetaServices,
		},
		pingIntervalMs: cc.PingMetaServiceIntervalMs,
		cache:          imeta.NewClient(mc),
	}
}

func (me *ClusterMetaClient) WithLogger(log *zap.Logger) {
	me.Logger = log.With(zap.String("coordinator", "ClusterMetaClient"))
}

func (me *ClusterMetaClient) Open() error {
	return me.cache.Open()
}

func (me *ClusterMetaClient) MarshalBinary() ([]byte, error) {
	return me.cache.MarshalBinary()
}

func (me *ClusterMetaClient) AcquireLease(name string) (*meta.Lease, error) {
	//TODO: pass node id
	return me.metaCli.AcquireLease(me.NodeID, name)
}

func (me *ClusterMetaClient) WaitForDataChanged() chan struct{} {
	return me.cache.WaitForDataChanged()
}

func (me *ClusterMetaClient) ClusterID() uint64 {
	return me.cache.ClusterID()
}

func (me *ClusterMetaClient) syncData() error {
	me.Logger.Info("start sync data")
	data, err := me.metaCli.Data()
	if err != nil {
		fmt.Println("start sync fail ", err)
		return err
	}

	me.Logger.Info("start sync done")
	return me.cache.ReplaceData(data)
}

func (me *ClusterMetaClient) syncLoop() {
	ticker := time.NewTicker(time.Duration(me.pingIntervalMs) * time.Millisecond)
	printLimiter := rate.NewLimiter(0.1, 1)
	for {
		select {
		case <-ticker.C:
			index, err := me.metaCli.Ping()
			if err != nil {
				me.Logger.Warn("Ping fail", zap.Error(err))
				continue
			}

			if index > me.cache.DataIndex() {
				if err := me.syncData(); err != nil {
					me.Logger.Warn("syncData fail", zap.Error(err))
				} else {
					me.Logger.Info("syncData success")
				}
			} else if index < me.cache.DataIndex() {
				me.Logger.Warn(fmt.Sprintf("index:%d < local index:%d", index, me.cache.DataIndex()))
			} else {
				// normal
				if printLimiter.Allow() {
					// one log in 10 seconds
					me.Logger.Debug(fmt.Sprintf("index=%d local_index=%d", index, me.cache.Data().Index))
				}
			}
		}
	}
}

func (me *ClusterMetaClient) Start() {
	// sync first synchronously
	wait := me.WaitForDataChanged()
	go me.syncData()
	//wait sync meta data from meta server
	select {
	case <-time.After(5 * time.Second):
		//TODO:
		panic("sync meta data failed")
	case <-wait:
	}
	go me.syncLoop()
}

func (me *ClusterMetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	if db, err := me.metaCli.CreateDatabase(name); err != nil {
		return db, err
	}
	return me.cache.CreateDatabase(name)
}

func (me *ClusterMetaClient) DeleteDataNode(id uint64) error {
	if err := me.metaCli.DeleteDataNode(id); err != nil {
		return err
	}
	return me.cache.DeleteDataNode(id)
}

func (me *ClusterMetaClient) IsDataNodeFreezed(id uint64) bool {
	return me.cache.IsDataNodeFreezed(id)
}

func (me *ClusterMetaClient) FreezeDataNode(id uint64) error {
	if err := me.metaCli.FreezeDataNode(id); err != nil {
		return err
	}
	return me.cache.FreezeDataNode(id)
}

func (me *ClusterMetaClient) UnfreezeDataNode(id uint64) error {
	if err := me.metaCli.UnfreezeDataNode(id); err != nil {
		return err
	}
	return me.cache.UnfreezeDataNode(id)
}

func (me *ClusterMetaClient) Database(name string) *meta.DatabaseInfo {
	return me.cache.Database(name)
}

func (me *ClusterMetaClient) AddShardOwner(shardID, nodeID uint64) error {
	if err := me.metaCli.AddShardOwner(shardID, nodeID); err != nil {
		return err
	}
	return me.cache.AddShardOwner(shardID, nodeID)
}

func (me *ClusterMetaClient) RemoveShardOwner(shardID, nodeID uint64) error {
	if err := me.metaCli.RemoveShardOwner(shardID, nodeID); err != nil {
		return err
	}
	return me.cache.RemoveShardOwner(shardID, nodeID)
}

func (me *ClusterMetaClient) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	if sg := me.cache.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}
	_, err := me.metaCli.CreateShardGroup(database, policy, timestamp)
	if err != nil {
		return nil, err
	}

	return me.cache.CreateShardGroup(database, policy, timestamp)
}

func (me *ClusterMetaClient) CreateDataNode(httpAddr, tcpAddr string) (*meta.NodeInfo, error) {
	if node, err := me.metaCli.CreateDataNode(httpAddr, tcpAddr); err != nil {
		return node, err
	}
	return me.cache.CreateDataNode(httpAddr, tcpAddr)
}

// DataNode returns the node information according to the id, if it's not existed a meta.ErrNodeNotFound is returned.
func (me *ClusterMetaClient) DataNode(id uint64) (*meta.NodeInfo, error) {
	return me.cache.DataNode(id)
}

func (me *ClusterMetaClient) DataNodes() ([]meta.NodeInfo, error) {
	return me.cache.DataNodes(), nil
}

func (me *ClusterMetaClient) ShardOwner(id uint64) (string, string, *meta.ShardGroupInfo) {
	return me.cache.ShardOwner(id)
}

func (me *ClusterMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	if db, err := me.metaCli.CreateDatabaseWithRetentionPolicy(name, spec); err != nil {
		return db, err
	}
	return me.cache.CreateDatabaseWithRetentionPolicy(name, spec)
}

func (me *ClusterMetaClient) CreateContinuousQuery(database, name, query string) error {
	if err := me.metaCli.CreateContinuousQuery(database, name, query); err != nil {
		return err
	}
	return me.cache.CreateContinuousQuery(database, name, query)
}

func (me *ClusterMetaClient) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	if ri, err := me.metaCli.CreateRetentionPolicy(database, spec, makeDefault); err != nil {
		return ri, err
	}
	return me.cache.CreateRetentionPolicy(database, spec, makeDefault)
}

func (me *ClusterMetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	if err := me.metaCli.CreateSubscription(database, rp, name, mode, destinations); err != nil {
		return err
	}
	return me.cache.CreateSubscription(database, rp, name, mode, destinations)
}

func (me *ClusterMetaClient) CreateUser(name, password string, admin bool) (u meta.User, err error) {
	u, err = me.metaCli.CreateUser(name, password, admin)
	return
}

func (me *ClusterMetaClient) Databases() []meta.DatabaseInfo {
	return me.cache.Databases()
}

func (me *ClusterMetaClient) DropShard(id uint64) error {
	if err := me.metaCli.DropShard(id); err != nil {
		return err
	}
	return me.cache.DropShard(id)
}

func (me *ClusterMetaClient) DropContinuousQuery(database, name string) error {
	if err := me.metaCli.DropContinuousQuery(database, name); err != nil {
		return err
	}
	return me.cache.DropContinuousQuery(database, name)
}

func (me *ClusterMetaClient) DropDatabase(name string) error {
	if err := me.metaCli.DropDatabase(name); err != nil {
		return err
	}
	return me.cache.DropDatabase(name)
}

func (me *ClusterMetaClient) DropRetentionPolicy(database, name string) error {
	if err := me.metaCli.DropRetentionPolicy(database, name); err != nil {
		return err
	}
	return me.cache.DropRetentionPolicy(database, name)
}

func (me *ClusterMetaClient) DropSubscription(database, rp, name string) error {
	if err := me.metaCli.DropSubscription(database, rp, name); err != nil {
		return err
	}
	return me.cache.DropSubscription(database, rp, name)
}

func (me *ClusterMetaClient) DropUser(name string) error {
	if err := me.metaCli.DropUser(name); err != nil {
		return err
	}
	return me.cache.DropUser(name)
}

func (me *ClusterMetaClient) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return me.cache.RetentionPolicy(database, name)
}

func (me *ClusterMetaClient) SetAdminPrivilege(username string, admin bool) error {
	if err := me.metaCli.SetAdminPrivilege(username, admin); err != nil {
		return err
	}
	return me.cache.SetAdminPrivilege(username, admin)
}

func (me *ClusterMetaClient) SetPrivilege(username, database string, p influxql.Privilege) error {
	if err := me.metaCli.SetPrivilege(username, database, p); err != nil {
		return err
	}
	return me.cache.SetPrivilege(username, database, p)
}

func (me *ClusterMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
	return me.cache.ShardGroupsByTimeRange(database, policy, min, max)
}

func (me *ClusterMetaClient) TruncateShardGroups(t time.Time) error {
	if err := me.metaCli.TruncateShardGroups(t); err != nil {
		return err
	}
	return me.cache.TruncateShardGroups(t)
}

func (me *ClusterMetaClient) DeleteShardGroup(database, policy string, id uint64) error {
	if err := me.metaCli.DeleteShardGroup(database, policy, id); err != nil {
		return err
	}
	// To cache data DeletedAt is not important
	return me.cache.DeleteShardGroup(database, policy, id, time.Now())
}

func (me *ClusterMetaClient) PruneShardGroups() error {
	if err := me.metaCli.PruneShardGroups(); err != nil {
		return err
	}
	return nil
}

func (me *ClusterMetaClient) PrecreateShardGroups(from, to time.Time) error {
	if err := me.metaCli.PrecreateShardGroups(from, to); err != nil {
		return err
	}
	return me.cache.PrecreateShardGroups(from, to)
}

func (me *ClusterMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	if err := me.metaCli.UpdateRetentionPolicy(database, name, rpu, makeDefault); err != nil {
		return err
	}
	return me.cache.UpdateRetentionPolicy(database, name, rpu, makeDefault)
}

func (me *ClusterMetaClient) UpdateUser(name, password string) error {
	if err := me.metaCli.UpdateUser(name, password); err != nil {
		return err
	}
	return nil
}

func (me *ClusterMetaClient) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return me.cache.UserPrivilege(username, database)
}

func (me *ClusterMetaClient) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return me.cache.UserPrivileges(username)
}

func (me *ClusterMetaClient) AdminUserExists() bool {
	return me.cache.AdminUserExists()
}

func (me *ClusterMetaClient) Authenticate(username, password string) (meta.User, error) {
	if u, err := me.metaCli.Authenticate(username, password); err != nil {
		return u, err
	}
	return me.cache.Authenticate(username, password)
}

func (me *ClusterMetaClient) UserCount() int {
	return me.cache.UserCount()
}

func (me *ClusterMetaClient) ShardIDs() []uint64 {
	return me.cache.ShardIDs()
}

func (me *ClusterMetaClient) Users() []meta.UserInfo {
	return me.cache.Users()
}

func (me *ClusterMetaClient) User(name string) (meta.User, error) {
	return me.cache.User(name)
}

func (me *ClusterMetaClient) DataNodeByTCPHost(addr string) (*meta.NodeInfo, error) {
	return me.cache.DataNodeByTCPHost(addr)
}
