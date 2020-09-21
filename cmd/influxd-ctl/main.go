package main

import (
	"fmt"
	"os"

	"github.com/angopher/chronus/cmd/influxd-ctl/command"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{}
	app.Name = "influxd-ctl"
	app.Usage = "Maintain the data nodes in cluster"
	app.ExitErrHandler = func(ctx *cli.Context, err error) {
		if err == nil {
			return
		}
		color.Red(err.Error())
		fmt.Println()
	}

	app.Commands = []*cli.Command{
		command.NodeCommand(),
		command.ShardCommand(),
	}
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "node",
			Aliases:     []string{"s"},
			Required:    true,
			Usage:       "DataNode address in cluster, ip:port",
			Destination: &command.DataNodeAddress,
		},
	}
	app.Run(os.Args)
}
