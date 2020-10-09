package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
)

// Data represents the top level collection of all metadata.
type Data struct {
	meta.Data
	MetaNodes        []meta.NodeInfo
	DataNodes        []meta.NodeInfo
	FreezedDataNodes []uint64 // data nodes that can't create new shard on

	MaxNodeID uint64
}

// DataNode returns a node by id.
func (data *Data) DataNode(id uint64) *meta.NodeInfo {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			// prevent unexpected modification
			n := data.DataNodes[i]
			return &n
		}
	}
	return nil
}

// CreateDataNode adds a node to the metadata, return the nodeId(0 when an error occurred) and error
func (data *Data) CreateDataNode(host, tcpHost string) (uint64, error) {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpHost || n.Host == host {
			return 0, ErrNodeExists
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost || n.Host == host {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, meta.NodeInfo{
		ID:      existingID,
		Host:    host,
		TCPHost: tcpHost,
	})
	sort.Sort(meta.NodeInfos(data.DataNodes))

	return existingID, nil
}

func existInNodes(nodes []meta.NodeInfo, id uint64) *meta.NodeInfo {
	for _, n := range nodes {
		if n.ID == id {
			return &n
		}
	}
	return nil
}

func getFreezed(freezed []uint64, id uint64) int {
	for i, n := range freezed {
		if n == id {
			return i
		}
	}
	return -1
}

func (data *Data) UnfreezeDataNode(id uint64) error {
	if id == 0 {
		return ErrNodeIDRequired
	}

	if existInNodes(data.DataNodes, id) == nil {
		return ErrNodeNotFound
	}

	i := getFreezed(data.FreezedDataNodes, id)
	if i == -1 {
		return ErrNodeNotFreezed
	}

	data.FreezedDataNodes = append(data.FreezedDataNodes[:i], data.FreezedDataNodes[i+1:]...)

	return nil
}

func (data *Data) IsFreezeDataNode(id uint64) bool {
	return getFreezed(data.FreezedDataNodes, id) > -1
}

func (data *Data) FreezeDataNode(id uint64) error {
	if id == 0 {
		return ErrNodeIDRequired
	}

	if existInNodes(data.DataNodes, id) == nil {
		return ErrNodeNotFound
	}

	if getFreezed(data.FreezedDataNodes, id) > -1 {
		return ErrNodeAlreadyFreezed
	}

	data.FreezedDataNodes = append(data.FreezedDataNodes, id)

	return nil
}

// DeleteDataNode removes a node from the Meta store.
//
// If necessary, DeleteDataNode reassigns ownership of any shards that
// would otherwise become orphaned by the removal of the node from the
// cluster.
func (data *Data) DeleteDataNode(id uint64) error {
	if id == 0 {
		return ErrNodeIDRequired
	}

	var nodes []meta.NodeInfo

	// Remove the data node from the store's list.
	for _, n := range data.DataNodes {
		if n.ID != id {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == len(data.DataNodes) {
		return ErrNodeNotFound
	}
	data.DataNodes = nodes

	// Remove node id from all shard infos
	for di, d := range data.Databases {
		for ri, rp := range d.RetentionPolicies {
			for sgi, sg := range rp.ShardGroups {
				var (
					nodeOwnerFreqs = make(map[int]int)
					orphanedShards []meta.ShardInfo
				)
				// Look through all shards in the shard group and
				// determine (1) if a shard no longer has any owners
				// (orphaned); (2) if all shards in the shard group
				// are orphaned; and (3) the number of shards in this
				// group owned by each data node in the cluster.
				for si, s := range sg.Shards {
					// Track of how many shards in the group are
					// owned by each data node in the cluster.
					var nodeIdx = -1
					for i, owner := range s.Owners {
						if owner.NodeID == id {
							nodeIdx = i
						}
						nodeOwnerFreqs[int(owner.NodeID)]++
					}

					if nodeIdx > -1 {
						// Data node owns shard, so relinquish ownership
						// and set new owners on the shard.
						s.Owners = append(s.Owners[:nodeIdx], s.Owners[nodeIdx+1:]...)
						data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards[si].Owners = s.Owners
					}

					// Shard no longer owned. Will need reassigning
					// an owner.
					if len(s.Owners) == 0 {
						orphanedShards = append(orphanedShards, s)
					}
				}

				// Mark the shard group as deleted if it has no shards,
				// or all of its shards are orphaned.
				if len(sg.Shards) == 0 || len(orphanedShards) == len(sg.Shards) {
					data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].DeletedAt = time.Now().UTC()
					continue
				}

				// Reassign any orphaned shards. Delete the node we're
				// dropping from the list of potential new owners.
				delete(nodeOwnerFreqs, int(id))

				for _, orphan := range orphanedShards {
					newOwnerID, err := newShardOwner(orphan, nodeOwnerFreqs)
					if err != nil {
						return err
					}

					for si, s := range sg.Shards {
						if s.ID == orphan.ID {
							sg.Shards[si].Owners = append(sg.Shards[si].Owners, meta.ShardOwner{NodeID: newOwnerID})
							data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards = sg.Shards
							break
						}
					}

				}
			}
		}
	}

	// Delete from freezed nodes if necessarily
	if i := getFreezed(data.FreezedDataNodes, id); i > -1 {
		data.FreezedDataNodes = append(data.FreezedDataNodes[:i], data.FreezedDataNodes[i+1:]...)
	}

	return nil
}

func cloneNodes(src []meta.NodeInfo) []meta.NodeInfo {
	if len(src) == 0 {
		return []meta.NodeInfo{}
	}
	nodes := make([]meta.NodeInfo, len(src))
	for i := range src {
		nodes[i] = src[i]
	}

	return nodes
}

// newShardOwner sets the owner of the provided shard to the data node
// that currently owns the fewest number of shards. If multiple nodes
// own the same (fewest) number of shards, then one of those nodes
// becomes the new shard owner.
func newShardOwner(s meta.ShardInfo, ownerFreqs map[int]int) (uint64, error) {
	var (
		minId   = -1
		minFreq int
	)

	for id, freq := range ownerFreqs {
		if minId == -1 || freq < minFreq {
			minId, minFreq = int(id), freq
		}
	}

	if minId < 0 {
		return 0, fmt.Errorf("cannot reassign shard %d due to lack of data nodes", s.ID)
	}

	// Update the shard owner frequencies and set the new owner on the
	// shard.
	ownerFreqs[minId]++
	return uint64(minId), nil
}

// MetaNode returns a node by id.
func (data *Data) MetaNode(id uint64) *meta.NodeInfo {
	for i := range data.MetaNodes {
		if data.MetaNodes[i].ID == id {
			// prevent unexpected modification
			n := data.MetaNodes[i]
			return &n
		}
	}
	return nil
}

// CreateMetaNode will add a new meta node to the metastore
func (data *Data) CreateMetaNode(httpAddr, tcpAddr string) (uint64, error) {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.MetaNodes {
		if n.Host == httpAddr {
			return 0, ErrNodeExists
		}
	}

	// If an existing data node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpAddr {
			existingID = n.ID
			break
		}
	}

	// We didn't find and existing data node ID, so assign a new ID
	// to this meta node.
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.MetaNodes = append(data.MetaNodes, meta.NodeInfo{
		ID:      existingID,
		Host:    httpAddr,
		TCPHost: tcpAddr,
	})

	sort.Sort(meta.NodeInfos(data.MetaNodes))
	return existingID, nil
}

// DeleteMetaNode will remove the meta node from the store
func (data *Data) DeleteMetaNode(id uint64) error {
	// Node has to be larger than 0 to be real
	if id == 0 {
		return ErrNodeIDRequired
	}

	var nodes []meta.NodeInfo
	for _, n := range data.MetaNodes {
		if n.ID == id {
			continue
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == len(data.MetaNodes) {
		return ErrNodeNotFound
	}

	data.MetaNodes = nodes
	return nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data

	other.Databases = data.CloneDatabases()
	other.Users = data.CloneUsers()
	other.DataNodes = cloneNodes(data.DataNodes)
	other.MetaNodes = cloneNodes(data.MetaNodes)
	other.FreezedDataNodes = make([]uint64, len(data.FreezedDataNodes))
	copy(other.FreezedDataNodes, data.FreezedDataNodes)

	return &other
}

type DataJson struct {
	Data      []byte
	MetaNodes []meta.NodeInfo
	DataNodes []meta.NodeInfo
	MaxNodeID uint64
}

func (data *Data) marshal() ([]byte, error) {
	var js DataJson
	js.MetaNodes = data.MetaNodes
	js.DataNodes = data.DataNodes
	js.MaxNodeID = data.MaxNodeID
	var err error
	js.Data, err = data.Data.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return json.Marshal(&js)
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) unmarshal(buf []byte) error {
	var js DataJson
	err := json.Unmarshal(buf, &js)
	if err != nil {
		return err
	}

	data.MetaNodes = js.MetaNodes
	data.DataNodes = js.DataNodes
	data.MaxNodeID = js.MaxNodeID
	return data.Data.UnmarshalBinary(js.Data)
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinary() ([]byte, error) {
	return data.marshal()
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
	data.unmarshal(buf)
	return nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (data *Data) CreateShardGroup(database, policy string, timestamp time.Time) error {
	// Ensure there are nodes in the metadata.
	if len(data.DataNodes) == 0 {
		return ErrNodeNotFound
	}

	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	} else if rpi == nil {
		return influxdb.ErrRetentionPolicyNotFound(policy)
	}

	// Verify that shard group doesn't already exist for this timestamp.
	if rpi.ShardGroupByTimestamp(timestamp) != nil {
		return nil
	}

	// Don't create shard on freezed nodes
	availableNodes := make([]meta.NodeInfo, 0, len(data.DataNodes))
	freezedNodes := make(map[uint64]bool)
	for _, n := range data.FreezedDataNodes {
		freezedNodes[n] = true
	}
	for _, n := range data.DataNodes {
		if freezedNodes[n.ID] {
			continue
		}
		availableNodes = append(availableNodes, n)
	}

	// Require at least one replica but no more replicas than nodes.
	replicaN := rpi.ReplicaN
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > len(availableNodes) {
		replicaN = len(availableNodes)
	}
	if replicaN < 1 {
		return errors.New("No replica can be assigned")
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := len(availableNodes) / replicaN

	// Create the shard group.
	data.MaxShardGroupID++
	sgi := meta.ShardGroupInfo{}
	sgi.ID = data.MaxShardGroupID
	sgi.StartTime = timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(rpi.ShardGroupDuration).UTC()
	if sgi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Shard group range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}

	// Create shards on the group.
	sgi.Shards = make([]meta.ShardInfo, shardN)
	for i := range sgi.Shards {
		data.MaxShardID++
		sgi.Shards[i] = meta.ShardInfo{ID: data.MaxShardID}
	}

	// Assign data nodes to shards via round robin.
	// Start from a repeatably "random" place in the node list.
	nodeIndex := int(data.Index % uint64(len(availableNodes)))
	for i := range sgi.Shards {
		si := &sgi.Shards[i]
		for j := 0; j < replicaN; j++ {
			nodeID := availableNodes[nodeIndex%len(availableNodes)].ID
			si.Owners = append(si.Owners, meta.ShardOwner{NodeID: nodeID})
			nodeIndex++
		}
	}

	// Retention policy has a new shard group, so update the policy. Shard
	// Groups must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	rpi.ShardGroups = append(rpi.ShardGroups, sgi)
	sort.Sort(meta.ShardGroupInfos(rpi.ShardGroups))

	return nil
}

func (data *Data) AddShardOwner(id, nodeID uint64) {
	for dbidx, dbi := range data.Databases {
		for rpidx, rpi := range dbi.RetentionPolicies {
			for sgidx, sg := range rpi.ShardGroups {
				for sidx, s := range sg.Shards {
					if s.ID == id {
						for _, owner := range s.Owners {
							if owner.NodeID == nodeID {
								return
							}
						}
						s.Owners = append(s.Owners, meta.ShardOwner{NodeID: nodeID})
						data.Databases[dbidx].RetentionPolicies[rpidx].ShardGroups[sgidx].Shards[sidx] = s
						return
					}
				}
			}
		}
	}
}

func (data *Data) RemoveShardOwner(id, nodeID uint64) {
	for dbidx, dbi := range data.Databases {
		for rpidx, rpi := range dbi.RetentionPolicies {
			for sgidx, sg := range rpi.ShardGroups {
				for sidx, s := range sg.Shards {
					var newOwners []meta.ShardOwner
					if s.ID == id {
						for _, owner := range s.Owners {
							if owner.NodeID != nodeID {
								newOwners = append(newOwners, owner)
							}
						}
						s.Owners = newOwners
						data.Databases[dbidx].RetentionPolicies[rpidx].ShardGroups[sgidx].Shards[sidx] = s
						return
					}
				}
			}
		}
	}
}
