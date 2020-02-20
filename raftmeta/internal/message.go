package internal

import (
	"github.com/coreos/etcd/raft/raftpb"
)

const (
	CreateDatabase                    = 1
	SnapShot                          = 2
	DropDatabase                      = 3
	DropRetentionPolicy               = 4
	CreateShardGroup                  = 5
	CreateDataNode                    = 6
	DeleteDataNode                    = 7
	CreateRetentionPolicy             = 8
	UpdateRetentionPolicy             = 9
	CreateDatabaseWithRetentionPolicy = 10
	CreateUser                        = 11
	DropUser                          = 12
	UpdateUser                        = 13
	SetPrivilege                      = 14
	SetAdminPrivilege                 = 15
	Authenticate                      = 16
	DropShard                         = 17
	TruncateShardGroups               = 18
	PruneShardGroups                  = 19
	DeleteShardGroup                  = 20
	PrecreateShardGroups              = 21
	CreateContinuousQuery             = 22
	DropContinuousQuery               = 23
	CreateSubscription                = 24
	DropSubscription                  = 25
	AcquireLease                      = 26
	Data                              = 27
	CreateChecksumMsg                 = 28
	VerifyChecksumMsg                 = 29
	AddShardOwner                     = 30
	RemoveShardOwner                  = 31
)

var MessageTypeName = map[int]string{
	1:  "CreateDatabase",
	2:  "SnapShot",
	3:  "DropDatabase",
	4:  "DropRetentionPolicy",
	5:  "CreateShardGroup",
	6:  "CreateDataNode",
	7:  "DeleteDataNode",
	8:  "CreateRetentionPolicy",
	9:  "UpdateRetentionPolicy",
	10: "CreateDatabaseWithRetentionPolicy",
	11: "CreateUser",
	12: "DropUser",
	13: "UpdateUser",
	14: "SetPrivilege",
	15: "SetAdminPrivilege",
	16: "Authenticate",
	17: "DropShard",
	18: "TruncateShardGroups",
	19: "PruneShardGroups",
	20: "DeleteShardGroup",
	21: "PrecreateShardGroups",
	22: "CreateContinuousQuery",
	23: "DropContinuousQuery",
	24: "CreateSubscription",
	25: "DropSubscription",
	26: "AcquireLease",
	27: "Data",
	28: "CreateChecksumMsg",
	29: "VerifyChecksumMsg",
	30: "AddShardOwner",
	31: "RemoveShardOwner",
}

type Proposal struct {
	Type int
	Key  string
	Data []byte
}

type EntryWrapper struct {
	Entry   raftpb.Entry
	Restore bool
}

type RaftContext struct {
	Addr string
	ID   uint64
}

type VerifyChecksum struct {
	Index    uint64
	Checksum string
	NodeID   uint64
}

type CreateSnapshot struct {
}

type SnapshotData struct {
	Data      []byte
	PeersAddr map[uint64]string
}
