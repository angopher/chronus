package cmds

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/angopher/chronus/cmd/metad-ctl/util"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func AddCommand() *cli.Command {
	return &cli.Command{
		Name:        "add",
		Usage:       "Add node to cluster",
		Description: "Introdue new node to cluster should follow two phases operation:\n   1. Add node to configuration using `metad-ctl add`\n   2. Boot up new node with confugration up to date",
		ArgsUsage:   "<new-node-id> <ip:port>",
		Action:      clusterAdd,
	}
}

func UpdateCommand() *cli.Command {
	return &cli.Command{
		Name:        "update",
		Usage:       "Update address of node in cluster",
		Description: "Update address of a node which is already in cluster should follow two phases operation:\n   1. Stop the node\n    2. Update address\n    3. Boot up node with confugration up to date",
		ArgsUsage:   "<new-node-id> <ip:port>",
		Action:      clusterUpdate,
	}
}

func RemoveCommand() *cli.Command {
	return &cli.Command{
		Name:        "remove",
		Usage:       "Remove node from cluster",
		Description: "Remove specified node from config.",
		ArgsUsage:   "<node-id>",
		Action:      clusterRemove,
	}
}

func clusterAdd(ctx *cli.Context) (err error) {
	var (
		id   uint64
		addr string
		data []byte
	)
	if ctx.Args().Len() < 2 {
		err = errors.New("Please specify node-id and address")
		goto ERR
	}
	id, addr, err = parseNodeIdAndAddr(ctx.Args().Get(0), ctx.Args().Get(1))
	if err != nil {
		goto ERR
	}

	data, err = util.PostRequestJSON(fmt.Sprint("http://", MetadAddress, "/update_cluster?op=add"), map[string]interface{}{
		"ID":   id,
		"Addr": addr,
	})
	if err != nil {
		goto ERR
	}
	err = processResponse(data)
	if err != nil {
		goto ERR
	}

	color.Green("Success")
	return nil

ERR:
	return err
}

func clusterUpdate(ctx *cli.Context) (err error) {
	var (
		id   uint64
		addr string
		data []byte
	)
	if ctx.Args().Len() < 2 {
		err = errors.New("Please specify node-id and address")
		goto ERR
	}
	id, addr, err = parseNodeIdAndAddr(ctx.Args().Get(0), ctx.Args().Get(1))
	if err != nil {
		goto ERR
	}

	data, err = util.PostRequestJSON(fmt.Sprint("http://", MetadAddress, "/update_cluster?op=update"), map[string]interface{}{
		"ID":   id,
		"Addr": addr,
	})
	if err != nil {
		goto ERR
	}
	err = processResponse(data)
	if err != nil {
		goto ERR
	}

	color.Green("Success")
	return nil

ERR:
	return err
}

func clusterRemove(ctx *cli.Context) (err error) {
	var (
		id   uint64
		data []byte
	)

	if ctx.Args().Len() < 1 {
		err = errors.New("Please specify node-id")
		goto ERR
	}

	id, err = parseNodeId(ctx.Args().Get(0))
	if err != nil {
		goto ERR
	}

	data, err = util.PostRequest(fmt.Sprint("http://", MetadAddress, "/update_cluster?op=remove"), url.Values{
		"node_id": {fmt.Sprint(id)},
	})
	if err != nil {
		goto ERR
	}
	err = processResponse(data)
	if err != nil {
		goto ERR
	}

	color.Green("Success")
	return nil

ERR:
	return err
}
