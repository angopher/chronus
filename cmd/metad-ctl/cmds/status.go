package cmds

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/angopher/chronus/cmd/metad-ctl/util"
	"github.com/angopher/chronus/raftmeta"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func dumpStatus(resp *raftmeta.StatusClusterResp) {
	color.Set(color.Bold)
	color.Green("Cluster:\n")
	fmt.Println("Leader:", resp.Leader)
	fmt.Println("Term:", resp.Term)
	fmt.Println("Committed:", resp.Commit)
	fmt.Println("Applied:", resp.Applied)
	fmt.Println()

	color.Set(color.Bold)
	color.Yellow("Nodes:\n")
	sort.Slice(resp.Nodes, func(i, j int) bool {
		return resp.Nodes[i].ID < resp.Nodes[j].ID
	})
	for _, n := range resp.Nodes {
		fmt.Print(util.PadRight(fmt.Sprint(n.ID), 6))
		fmt.Print(" ", util.PadRight(n.Role, 15))
		fmt.Print(util.PadRight(n.Addr, 23))
		fmt.Print(util.PadRight(n.Progress, 15))
		fmt.Print(util.PadRight(fmt.Sprint(n.Match, "=>", n.Next), 20))
		if n.Vote > 0 {
			fmt.Print("Vote(", n.Vote, ")")
		}
		fmt.Println()
	}
}

func StatusCommand() *cli.Command {
	return &cli.Command{
		Name:        "status",
		Usage:       "status of cluster",
		Description: "Get the cluster status",
		Action:      clusterStatus,
		Flags:       []cli.Flag{FLAG_ADDR},
	}
}

func clusterStatus(ctx *cli.Context) (err error) {
	resp := &raftmeta.StatusClusterResp{}
	data, err := util.GetRequest(fmt.Sprint("http://", MetadAddress, "/status_cluster"))
	if err != nil {
		goto ERR
	}

	err = json.Unmarshal(data, resp)
	if err != nil {
		goto ERR
	}
	if resp.RetCode != 0 {
		err = errors.New(resp.RetMsg)
		goto ERR
	}

	dumpStatus(resp)
	return nil

ERR:
	return err
}
