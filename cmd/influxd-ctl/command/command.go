package command

import (
	"fmt"
	"github.com/angopher/chronus/cmd/influxd-ctl/action"
	"github.com/spf13/cobra"
)

const (
	defaultHost = "127.0.0.1:8088"
)

func NewCommand() *cobra.Command {
	var copyShardCmd = &cobra.Command{
		Use:   "copy-shard <data-node-source-TCP-address> <data-node-destination-TCP-address> <shard-id>",
		Short: "copy shard",
		Long:  `copies a shard from a source data node to a destination data node.`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			action.CopyShard(args[0], args[1], args[2])
		},
	}

	var truncateCmd = &cobra.Command{
		Use:   "truncate-shards <delay-seconds> [ip:port]",
		Short: "truncates hot shards",
		Long:  `Truncates hot shards, that is, shards that cover the time range that includes the current time (now()).The truncate-shards command creates a new shard and the system writes all new points to that shard.`,
		Args:  cobra.RangeArgs(1, 2),
		Run: func(cmd *cobra.Command, args []string) {
			host := defaultHost
			if len(args) == 2 {
				host = args[1]
			}
			if err := action.TruncateShards(args[0], host); err != nil {
				fmt.Println(err)
			}
		},
	}

	var copyShardStatusCmd = &cobra.Command{
		Use:   "copy-shard-status [ip:port]",
		Short: "Displaying all in-progress copy-shard operations",
		Long: `Shows all in-progress copy shard operations, including the shard’s source node,
		destination node, database, retention policy, shard ID, total size,
		current size, and the operation’s start time.`,
		Args: cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			host := defaultHost
			if len(args) == 1 {
				host = args[0]
			}
			if err := action.CopyShardStatus(host); err != nil {
				fmt.Println(err)
			}
		},
	}

	var killCopyShardCmd = &cobra.Command{
		Use:   "kill-copy-shard <data-node-source-TCP-address> <data-node-destination-TCP-address> <shard-ID>",
		Short: "Aborts an in-progress copy-shard command.",
		Long:  "Aborts an in-progress copy-shard command.",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			if err := action.KillCopyShard(args[0], args[1], args[2]); err != nil {
				fmt.Println(err)
			}
		},
	}

	var removeShardCmd = &cobra.Command{
		Use:   "remove-shard <data-node-source-TCP-address> <shard-ID>",
		Short: "Removes a shard from a data node. Removing a shard is an irrecoverable, destructive action; please be cautious with this command.",
		Long:  "Removes a shard from a data node. Removing a shard is an irrecoverable, destructive action; please be cautious with this command.",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if err := action.RemoveShard(args[0], args[1]); err != nil {
				fmt.Println(err)
			}
		},
	}

	var removeDataNodeCmd = &cobra.Command{
		Use:   "remove-data-node <data-node-source-TCP-address>",
		Short: "Removes a data node from a cluster.",
		Long:  "Removes a data node from a cluster.",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := action.RemoveDataNode(args[0]); err != nil {
				fmt.Println(err)
			}
		},
	}

	var showDataNodesCmd = &cobra.Command{
		Use:   "show-data-nodes [ip:port]",
		Short: "Show all data node from a cluster.",
		Long:  "Show all data node from a cluster.",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			host := defaultHost
			if len(args) == 1 {
				host = args[0]
			}
			if err := action.ShowDataNodes(host); err != nil {
				fmt.Println(err)
			}
		},
	}

	var rootCmd = &cobra.Command{Use: "influxd-ctl"}
	rootCmd.AddCommand(
		copyShardCmd,
		truncateCmd,
		copyShardStatusCmd,
		killCopyShardCmd,
		removeShardCmd,
		removeDataNodeCmd,
		showDataNodesCmd,
	)

	return rootCmd
}
