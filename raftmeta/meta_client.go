package raftmeta

import (
	"time"

	imeta "github.com/angopher/chronus/services/meta"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

type MetaClient interface {
	MarshalBinary() ([]byte, error)
	ReplaceData(data *imeta.Data) error
	Data() imeta.Data
	CreateContinuousQuery(database, name, query string) error
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	CreateSubscription(database, rp, name, mode string, destinations []string) error
	CreateUser(name, password string, admin bool) (meta.User, error)
	CreateDataNode(httpAddr, tcpAddr string) (*meta.NodeInfo, error)
	DeleteDataNode(id uint64) error
	IsDataNodeFreezed(id uint64) bool
	FreezeDataNode(id uint64) error
	UnfreezeDataNode(id uint64) error
	Authenticate(username, password string) (meta.User, error)
	PruneShardGroups() error
	DeleteShardGroup(database, policy string, id uint64) error
	PrecreateShardGroups(from, to time.Time) error

	AddShardOwner(shardID, nodeID uint64) error
	RemoveShardOwner(shardID, nodeID uint64) error
	DropShard(id uint64) error
	DropContinuousQuery(database, name string) error
	DropDatabase(name string) error
	DropRetentionPolicy(database, name string) error
	DropSubscription(database, rp, name string) error
	DropUser(name string) error
	SetAdminPrivilege(username string, admin bool) error
	SetPrivilege(username, database string, p influxql.Privilege) error
	TruncateShardGroups(t time.Time) error
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	UpdateUser(name, password string) error
}
