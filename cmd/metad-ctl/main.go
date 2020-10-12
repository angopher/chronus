package main

import (
	"fmt"
	"os"

	"github.com/angopher/chronus/cmd/metad-ctl/cmds"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{}
	app.Name = "metad-ctl"
	app.Usage = "Maintain the metad cluster"
	app.ExitErrHandler = func(ctx *cli.Context, err error) {
		if err == nil {
			return
		}
		color.Red(err.Error())
		fmt.Println()
	}

	app.Commands = []*cli.Command{
		cmds.StatusCommand(),
		cmds.AddCommand(),
		cmds.UpdateCommand(),
		cmds.RemoveCommand(),
	}
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "metad",
			Aliases:     []string{"s"},
			Required:    true,
			Usage:       "Node address in cluster, ip:port",
			Destination: &cmds.MetadAddress,
		},
	}
	app.Run(os.Args)
}
