package command

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/angopher/chronus/cmd/influxd-ctl/action"
	"github.com/urfave/cli/v2"
)

const (
	defaultHost = "127.0.0.1:8088"
)

var (
	DataNodeAddress string
)

func NodeCommand() *cli.Command {
	return &cli.Command{
		Name:  "node",
		Usage: "node related operations",
		Subcommands: []*cli.Command{
			{
				Name:  "list",
				Usage: "show nodes in cluster",
				Action: func(ctx *cli.Context) error {
					if err := action.ShowDataNodes(DataNodeAddress); err != nil {
						fmt.Println(err)
					}
					return nil
				},
			}, {
				Name:      "freeze",
				ArgsUsage: "freeze <ip:port>",
				Usage:     "freeze specified node preventing creating new shard on",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify node addr to be freezed")
					}
					if err := action.FreezeDataNode(DataNodeAddress, ctx.Args().Get(0)); err != nil {
						fmt.Println(err)
					}

					return nil
				},
			}, {
				Name:      "unfreeze",
				ArgsUsage: "unfreeze <ip:port>",
				Usage:     "unfreeze specified node",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify node addr to unfreeze")
					}
					if err := action.UnfreezeDataNode(DataNodeAddress, ctx.Args().Get(0)); err != nil {
						fmt.Println(err)
					}

					return nil
				},
			}, {
				Name:      "remove",
				ArgsUsage: "remove <ip:port>",
				Usage:     "remove specified node from cluster",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify node addr to be removed from cluster")
					}
					if err := action.RemoveDataNode(DataNodeAddress, ctx.Args().Get(0)); err != nil {
						fmt.Println(err)
					}

					return nil
				},
			},
		},
	}
}

func ShardCommand() *cli.Command {
	return &cli.Command{
		Name:  "shard",
		Usage: "shard related operations",
		Subcommands: []*cli.Command{
			{
				Name:      "list",
				ArgsUsage: "list <db> <rp>",
				Usage:     "show all shards of specified retention policy",
				Description: fmt.Sprint(
					"List all shards in specified retention policy.",
				),
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return errors.New("Please specify database and retention policy")
					}
					if err := action.ListShard(DataNodeAddress, ctx.Args().Get(0), ctx.Args().Get(1)); err != nil {
						fmt.Println(err)
					}
					return nil

				},
			}, {
				Name:      "node",
				ArgsUsage: "node <node id>",
				Usage:     "show all shards' id on specified node",
				Description: fmt.Sprint(
					"List all shards on node.",
				),
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify node id")
					}
					nodeId, _ := strconv.ParseUint(ctx.Args().Get(0), 10, 64)
					if nodeId < 1 {
						return errors.New("Please specify node id")
					}
					if err := action.ListShardOnNode(DataNodeAddress, nodeId); err != nil {
						fmt.Println(err)
					}
					return nil

				},
			}, {
				Name:      "info",
				ArgsUsage: "info <shard-id>",
				Usage:     "show information of specified shard",
				Description: fmt.Sprint(
					"Show information of specified shard.",
				),
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify shard id")
					}
					if err := action.GetShard(DataNodeAddress, ctx.Args().Get(0)); err != nil {
						fmt.Println(err.Error())
					}
					return nil
				},
			}, {
				Name:  "status",
				Usage: "show progress of copy-shard tasks",
				Description: fmt.Sprint(
					"Shows all in-progress copy shard operations, including the shard’s source node,\n",
					"destination node, database, retention policy, shard ID, total size,\n",
					"current size, and the operation’s start time.",
				),
				Action: func(ctx *cli.Context) error {
					if err := action.CopyShardStatus(DataNodeAddress); err != nil {
						fmt.Println(err)
					}
					return nil
				},
			}, {
				Name:        "copy",
				Usage:       "copy a shard to current node",
				ArgsUsage:   "copy <source-tcp-addr> <shard-id>",
				Description: "Copy a shard from a source data node to a current data node which is specified through -s",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return errors.New("Please specify source node and shard")
					}
					action.CopyShard(ctx.Args().Get(0), DataNodeAddress, ctx.Args().Get(1))
					return nil
				},
			}, {
				Name:      "remove",
				Usage:     "remove a shard",
				ArgsUsage: "remove <shard-id>",
				Description: fmt.Sprint(
					"Removes a shard from current data node.\n",
					"Removing a shard is an irrecoverable, destructive action;\n",
					"Please be cautious with this command.",
				),
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("Please specify shard")
					}
					if err := action.RemoveShard(DataNodeAddress, ctx.Args().First()); err != nil {
						fmt.Println(err)
					}
					return nil
				},
			}, {
				Name:        "stop",
				Usage:       "stop a task of copy shard",
				ArgsUsage:   "stop <source-tcp-addr> <shard-id>",
				Description: "Stop a task of copy shard to current node which specified through -s.",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return errors.New("Please specify source node and shard")
					}
					if err := action.KillCopyShard(ctx.Args().Get(0), DataNodeAddress, ctx.Args().Get(1)); err != nil {
						fmt.Println(err)
					}
					return nil
				},
			}, {
				Name:      "truncate",
				Usage:     "truncates hot shards",
				ArgsUsage: "truncate <delay-seconds>",
				Description: fmt.Sprint(
					"Truncates hot shards, that is, shards that cover the time range\n",
					"that includes the current time (now()).\n",
					"The truncate-shards command creates a new shard and \n",
					"the system writes all new points to that shard.",
				),
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return errors.New("delay seconds should be specified")
					}
					if err := action.TruncateShards(ctx.Args().Get(0), DataNodeAddress); err != nil {
						fmt.Println(err)
					}
					return nil
				},
			},
		},
	}
}
