package main

import (
	"github.com/angopher/chronus/cmd/influxd-ctl/command"
)

func main() {
	command := command.NewCommand()
	command.Execute()
}
