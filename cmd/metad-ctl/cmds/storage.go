package cmds

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/angopher/chronus/cmd/metad-ctl/util"
	"github.com/angopher/chronus/raftmeta"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/table"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func dumpStatus1(resp *raftmeta.StatusClusterResp) {
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

func StorageCommand() *cli.Command {
	return &cli.Command{
		Name:        "storage",
		Usage:       "View storage info",
		Description: "Get the storage info",
		Action:      storageInfo,
	}
}

func storageInfo(ctx *cli.Context) (err error) {
	if ctx.Args().Len() < 1 {
		return errors.New("Usage: metad-ctl storage <wal data dir>")
	}

	walDir := ctx.Args().First()
	fmt.Println("Dump information of", walDir)

	fileinfos, err := ioutil.ReadDir(walDir)
	if err != nil {
		return err
	}
	fmt.Println("Collect files ...")
	fileinfoByName := make(map[string]os.FileInfo)
	for _, info := range fileinfos {
		fileinfoByName[info.Name()] = info
	}

	fmt.Println("Reading", badger.ManifestFilename, "...")
	fp, err := os.Open(filepath.Join(walDir, badger.ManifestFilename))
	if err != nil {
		return err
	}
	defer fp.Close()

	manifest, _, err := badger.ReplayManifestFile(fp)
	fmt.Println()
	color.Set(color.Bold)
	color.Green("Disk space cost by level:")
	for level, lm := range manifest.Levels {
		sz := int64(0)
		for id := range lm.Tables {
			tableFile := table.IDToFilename(id)
			if info, ok := fileinfoByName[tableFile]; ok {
				sz += info.Size()
			}
		}
		fmt.Print("Level ", level, ": ", humanize.Bytes(uint64(sz)), "\n")
	}

	return nil
}
