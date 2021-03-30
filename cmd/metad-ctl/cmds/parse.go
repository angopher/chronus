package cmds

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/angopher/chronus/raftmeta"
	"github.com/urfave/cli/v2"
)

var (
	FLAG_ADDR = &cli.StringFlag{
		Name:        "metad",
		Aliases:     []string{"s"},
		Required:    true,
		Usage:       "Node address in cluster, ip:port",
		Destination: &MetadAddress,
	}
)

func parseNodeAddr(arg string) (string, error) {
	if len(arg) < 6 || !strings.Contains(arg, ":") {
		return "", errors.New("Incorrect address, should be ip:port")
	}
	return arg, nil
}

func parseNodeId(arg string) (uint64, error) {
	id, err := strconv.ParseUint(arg, 10, 64)
	if err != nil {
		return 0, err
	}
	if id < 1 {
		err = errors.New("Node id should be positive")
		return 0, err
	}
	return id, nil
}

func parseNodeIdAndAddr(arg0, arg1 string) (id uint64, addr string, err error) {
	id, err = parseNodeId(arg0)
	if err != nil {
		return
	}

	addr, err = parseNodeAddr(arg1)
	if err != nil {
		return
	}

	return
}

func processResponse(data []byte) error {
	resp := raftmeta.CommonResp{}
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return err
	}

	if resp.RetCode != 0 {
		return errors.New(resp.RetMsg)
	}

	return nil
}
