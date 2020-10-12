package action

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/angopher/chronus/cmd/metad-ctl/util"
	"github.com/angopher/chronus/coordinator"
	"github.com/angopher/chronus/services/controller"
	"github.com/fatih/color"
)

func CopyShard(srcAddr, dstAddr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.CopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.CopyShardResponse
	respTyp := byte(controller.ResponseCopyShard)
	reqTyp := byte(controller.RequestCopyShard)
	if err := RequestAndWaitResp(dstAddr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func TruncateShards(delay string, addr string) error {
	delaySec, err := strconv.ParseInt(delay, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.TruncateShardRequest{DelaySec: delaySec}
	reqTyp := byte(controller.RequestTruncateShard)

	respTyp := byte(controller.ResponseTruncateShard)
	var resp controller.CopyShardStatusResponse
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func ListShardOnNode(addr string, nodeId uint64) error {
	var req controller.GetNodeShardsRequest
	var resp controller.NodeShardsResponse
	respTyp := byte(controller.ResponseNodeShards)
	reqTyp := byte(controller.RequestNodeShards)
	req.NodeID = nodeId
	var err error
	if err = RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return errors.New(resp.Msg)
	}

	color.Set(color.Bold)
	color.Green(fmt.Sprint("Shards on node ", req.NodeID, ":\n"))
	fmt.Println(resp.Shards)
	fmt.Println()
	return nil
}

func ListShard(addr, db, rp string) error {
	var req controller.GetShardsRequest
	var resp controller.ShardsResponse
	respTyp := byte(controller.ResponseShards)
	reqTyp := byte(controller.RequestShards)
	req.Database = db
	req.RetentionPolicy = rp
	nodes, err := getNodes(addr)
	if err != nil {
		return err
	}
	if err = RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return errors.New(resp.Msg)
	}

	if resp.Rp == "" {
		return errors.New("Specified retention policy could not be found")
	}

	color.Set(color.Bold)
	color.Green("Retenion Policy [%s]:\n", resp.Rp)
	fmt.Println("Replica:", resp.Replica)
	fmt.Println("Duration:", resp.Duration)
	fmt.Println("Group Duration:", resp.GroupDuration)
	fmt.Println()

	color.Set(color.Bold)
	color.Green("Groups\n")
	color.Yellow(fmt.Sprint(
		util.PadRight("GroupId", 10),
		util.PadRight("Start", 21),
		util.PadRight("End", 21),
		util.PadRight("DeletedAt", 21),
		util.PadRight("TruncatedAt", 19),
		"\n",
	))
	for _, g := range resp.Groups {
		fmt.Print(
			util.PadRight(fmt.Sprint(g.ID), 10),
			formatTimeStamp(g.StartTime),
			"  ", formatTimeStamp(g.EndTime),
			"  ", formatTimeStamp(g.DeletedAt),
			"  ", formatTimeStamp(g.TruncatedAt),
			"\n",
		)
		for _, shard := range g.Shards {
			fmt.Print(color.CyanString("      |--Shard: %d\tNodes: [", shard.ID))
			first := true
			for _, id := range shard.Nodes {
				if n, ok := nodes[id]; ok {
					if !first {
						fmt.Print(color.CyanString(", "))
					}
					fmt.Print(color.CyanString("%s", n.TcpAddr))
					first = false
				}
			}
			fmt.Println(color.CyanString("]"))
		}
		if len(g.Shards) == 0 {
			fmt.Println("No shard")
		}
	}
	fmt.Println()
	return nil
}

func GetShard(addr, shard string) error {
	var req controller.GetShardRequest
	var resp controller.ShardResponse
	respTyp := byte(controller.ResponseShard)
	reqTyp := byte(controller.RequestShard)
	id, err := strconv.ParseUint(shard, 10, 64)
	if err != nil || id < 1 {
		return errors.New("Please specify correct shard id")
	}
	req.ShardID = id
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}
	if resp.Code != 0 {
		return errors.New(resp.Msg)
	}

	color.Set(color.Bold)
	fmt.Println(color.GreenString("Shard:"), resp.ID)
	color.Set(color.Bold)
	fmt.Println(color.GreenString("Database:"), resp.DB)
	color.Set(color.Bold)
	fmt.Println(color.GreenString("Retenion Policy:"), resp.Rp)
	color.Set(color.Bold)
	fmt.Print(color.GreenString("Nodes: "))
	fmt.Printf("%v\n", resp.Nodes)
	fmt.Println()
	return nil
}

func CopyShardStatus(addr string) error {
	var resp controller.CopyShardStatusResponse
	respTyp := byte(controller.ResponseCopyShardStatus)
	reqTyp := byte(controller.RequestCopyShardStatus)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, struct{}{}, &resp); err != nil {
		return err
	}

	color.Set(color.Bold)
	color.Green("Running Copy Tasks:\n")
	for _, t := range resp.Tasks {
		fmt.Print(t.ShardID, "\t", t.Database, "\t", t.Rp, "\t", t.CurrentSize, "/", t.TotalSize, "\t", t.Source, "\n")
	}
	fmt.Println()
	return nil
}

func KillCopyShard(srcAddr, dstAddr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.KillCopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.KillCopyShardResponse
	respTyp := byte(controller.ResponseKillCopyShard)
	reqTyp := byte(controller.RequestKillCopyShard)
	if err := RequestAndWaitResp(dstAddr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func RemoveShard(addr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.RemoveShardRequest{
		DataNodeAddr: addr,
		ShardID:      id,
	}

	var resp controller.RemoveShardResponse
	respTyp := byte(controller.ResponseRemoveShard)
	reqTyp := byte(controller.RequestRemoveShard)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func RemoveDataNode(addr, removed_addr string) error {
	req := &controller.RemoveDataNodeRequest{
		DataNodeAddr: removed_addr,
	}

	var resp controller.RemoveDataNodeResponse
	respTyp := byte(controller.ResponseRemoveDataNode)
	reqTyp := byte(controller.RequestRemoveDataNode)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	color.Set(color.Bold)
	color.Green("Result: ")
	fmt.Println(resp.Msg)
	return nil
}

func freezeDataNode(addr, freezed_addr string, freeze bool) error {
	req := &controller.FreezeDataNodeRequest{
		DataNodeAddr: freezed_addr,
		Freeze:       freeze,
	}

	var resp controller.FreezeDataNodeResponse
	respTyp := byte(controller.ResponseFreezeDataNode)
	reqTyp := byte(controller.RequestFreezeDataNode)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	color.Set(color.Bold)
	color.Green("Result: ")
	fmt.Println(resp.Msg)
	return nil
}

func FreezeDataNode(addr, freezed_addr string) error {
	return freezeDataNode(addr, freezed_addr, true)
}

func UnfreezeDataNode(addr, freezed_addr string) error {
	return freezeDataNode(addr, freezed_addr, false)
}

func getNodes(addr string) (map[uint64]controller.DataNode, error) {
	var resp controller.ShowDataNodesResponse
	respTyp := byte(controller.ResponseShowDataNodes)
	reqTyp := byte(controller.RequestShowDataNodes)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, struct{}{}, &resp); err != nil {
		return nil, err
	}
	nodes := make(map[uint64]controller.DataNode)
	for _, n := range resp.DataNodes {
		nodes[n.ID] = n
	}
	return nodes, nil
}

func ShowDataNodes(addr string) error {
	color.Set(color.Bold)
	color.Green("Nodes:\n")
	nodes, err := getNodes(addr)
	if err != nil {
		return err
	}
	ids := make([]uint64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		n := nodes[id]
		fmt.Print(n.ID, "\thttp://", n.HttpAddr, "\ttcp://", n.TcpAddr)
		if n.Freezed {
			fmt.Print("\t(freezed)")
		}
		fmt.Print("\n")
	}
	fmt.Println()
	return nil
}

func RequestAndWaitResp(addr string, reqTyp, respTyp byte, req interface{}, resp interface{}) error {
	conn, err := Dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	buf, _ := json.Marshal(req)
	if err := coordinator.WriteTLV(conn, reqTyp, buf); err != nil {
		return err
	}

	return DecodeTLV(conn, respTyp, resp)
}

func DecodeTLV(r io.Reader, expTyp byte, v interface{}) error {
	typ, err := coordinator.ReadType(r)
	if err != nil {
		return err
	}
	if expTyp != typ {
		return fmt.Errorf("invalid type, exp: %d, got: %d", expTyp, typ)
	}

	buf, err := coordinator.ReadLV(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return err
	}
	return nil
}

func Dial(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(time.Second))

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{controller.MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})

	return conn, nil
}
