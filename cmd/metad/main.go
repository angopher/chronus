package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/angopher/chronus/logging"
	"github.com/angopher/chronus/raftmeta"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/services/meta"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

func main() {
	f := flag.NewFlagSet("metad", flag.ExitOnError)
	configFile := f.String("config", "", "Specify config file")
	dumpFile := f.String("dump", "", "Boot and dump the snapshot to a file specified")
	restoreFile := f.String("restore", "", "Boot and restore data from the snapshot specified")
	showSample := f.Bool("sample", false, "Show sample configuration")
	f.Parse(os.Args[1:])

	config := raftmeta.NewConfig()
	if *showSample {
		toml.NewEncoder(os.Stdout).Encode(&config)
		fmt.Println()
		return
	}

	if *configFile != "" {
		x.Check((&config).FromTomlFile(*configFile))
	} else {
		f.Usage()
		return
	}

	metaCli := imeta.NewClient(&meta.Config{
		RetentionAutoCreate: config.RetentionAutoCreate,
		LoggingEnabled:      true,
	})
	log, err := logging.InitialLogging(&logging.Config{
		Format:   config.LogFormat,
		Level:    config.LogLevel,
		Dir:      config.LogDir,
		FileName: "metad.log",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error to initialize logging", err)
		return
	}

	suger := log.Sugar()
	suger.Debug("config: %+v", config)

	metaCli.WithLogger(log)
	err = metaCli.Open()
	x.Check(err)

	node := raftmeta.NewRaftNode(config, log)
	node.MetaCli = metaCli

	// dump only
	if *dumpFile != "" {
		err := node.Dump(*dumpFile)
		if err != nil {
			fmt.Println("Dump to file error:", err)
			return
		}
		fmt.Println("Dumped to", *dumpFile)
		return
	}

	// restore
	if *restoreFile != "" {
		// set conf state first
		var ids []uint64
		for _, n := range node.Config.Peers {
			ids = append(ids, n.RaftId)
		}
		node.SetConfState(&raftpb.ConfState{
			Voters: ids,
		})
		err = node.Restore(*restoreFile)
		if err != nil {
			node.Logger.Warn("Restore from file failed", zap.Error(err))
			return
		}
	}

	t := raftmeta.NewTransport()
	t.WithLogger(log)

	node.Transport = t
	node.InitAndStartNode()
	go node.Run()

	//线性一致性读
	linearRead := raftmeta.NewLinearizabler(node)
	go linearRead.ReadLoop()

	service := raftmeta.NewMetaService(config.MyAddr, metaCli, node, linearRead)
	service.InitRouter()
	service.WithLogger(log)
	service.Start()
}
