package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/angopher/chronus/services/migrate"
	"github.com/angopher/chronus/x"
)

func (s *Service) copyShard(sourceAddr string, shardId uint64) error {
	task := migrate.Task{}
	task.SrcHost = sourceAddr
	task.ShardId = shardId

	db, rp, sgi := s.MetaClient.ShardOwner(shardId)
	if sgi == nil {
		return fmt.Errorf("shard %d not exists", shardId)
	}
	task.Database = db
	task.Retention = rp

	path := filepath.Join(s.TSDBStore.Path(), db, rp, strconv.FormatUint(shardId, 10))
	if x.Exists(path) != x.NotExisted {
		return fmt.Errorf("local shard:[%s] exists", path)
	}
	task.DstStorePath = path

	copyDir := filepath.Join(s.TSDBStore.Path(), ".copy_shard")
	os.MkdirAll(copyDir, 0755)
	task.TmpStorePath = copyDir

	err := s.migrateManager.Add(&task)
	if err != nil {
		return err
	}

	err = <-task.C

	if err != nil {
		return err
	}

	sh := s.TSDBStore.Shard(shardId)
	if sh != nil {
		return fmt.Errorf("Shard %d is existed which is unexpected after backuping", shardId)
	}

	err = s.TSDBStore.CreateShard(task.Database, task.Retention, task.ShardId, true)
	if err != nil {
		return err
	}

	return s.MetaClient.AddShardOwner(task.ShardId, s.Node.ID)
}
