package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/pkg/tar"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const (
	MAX_RETRY = 3
)

type ValidatorFn func(file string) error
type ProgressCB func(copied uint64)

type ManagerInterface interface {
	Add(task *Task) error
	Remove(id uint64)
	Tasks() []*Task
	Kill(id uint64, source, dest string)
}

type MetaClientInterface interface {
	ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
	AddShardOwner(shardID, nodeID uint64) error
}

type TSDBInterface interface {
	Path() string
	ShardRelativePath(id uint64) (string, error)
	CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
	Shard(id uint64) *tsdb.Shard
}

func (m *Manager) execute(t *Task) (err error) {
	t.Started = true
	t.StartTime = time.Now().Unix()
	defer func() {
		if !t.Finished {
			t.error(fmt.Errorf("Unknow error for task on shard %d", t.ShardId))
		}
	}()
	i := 1
	delay := 2 * time.Second
	for i <= MAX_RETRY {
		err = m.replicate(t)
		if err == nil {
			return
		}
		time.Sleep(delay)
		// backoff by 2
		delay *= 2
		i++
	}
	return fmt.Errorf("Migration for shard %d failed after retries", t.ShardId)
}

func backupFromRemote(t *Task, logger *zap.SugaredLogger) (*os.File, error) {
	// prepare local tmp file
	tmpFilePath := filepath.Join(t.TmpStorePath, fmt.Sprint("shard_", t.ShardId))
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return nil, fmt.Errorf("open temp file failed: %s", err)
	}
	defer tmpFile.Close()
	// Connect to snapshotter service.
	conn, err := tcp.Dial("tcp", t.SrcHost, snapshotter.MuxHeader)
	if err != nil {
		return tmpFile, err
	}
	defer conn.Close()
	logger.Info("Connected to ", t.SrcHost)

	req := &snapshotter.Request{
		Type:                  snapshotter.RequestShardBackup,
		BackupDatabase:        t.Database,
		BackupRetentionPolicy: t.Retention,
		ShardID:               t.ShardId,
	}

	// Write the request type
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		return tmpFile, err
	}

	// Write the request
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return tmpFile, fmt.Errorf("encode snapshot request: %s", err)
	}

	buf := make([]byte, 1024*1024)
	n := 0
	ctx := context.Background()
	for {
		t.Limiter.Allow()
		n, err = conn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			break
		}
		tmpFile.Write(buf[:n])
		t.Copied += uint64(n)
		// limiter in post way
		t.Limiter.WaitN(ctx, n)
		if t.ProgressLimiter.Allow() {
			logger.Infof("Migrating shard %d: %d copied", t.ShardId, t.Copied)
		}
	}
	logger.Infof("Transfer Shard %d completely with %d bytes", t.ShardId, t.Copied)

	// XXX: Verify
	return tmpFile, nil
}

func (m *Manager) replicate(t *Task) error {
	if x.Exists(t.DstStorePath) != x.NotExisted {
		t.error(errors.New("Destination shard directory has already existed"))
		return nil
	}
	if x.Exists(t.TmpStorePath) == x.NotExisted {
		t.error(errors.New("Temporary directory is not existed"))
		return nil
	}

	// transfer to local
	m.logger.Infof("start to execute task copying %d from %s", t.ShardId, t.SrcHost)
	tmpFile, err := backupFromRemote(t, m.logger)
	defer func() {
		// remove tmp file
		if tmpFile != nil {
			os.Remove(tmpFile.Name())
		}
	}()
	if err != nil {
		return err
	}

	// unpack to destination
	os.MkdirAll(t.DstStorePath, os.FileMode(0755))
	err = restoreFromTar(tmpFile.Name(), t.DstStorePath)
	if err != nil {
		m.logger.Errorf("Unpack from backup failed for shard %d: %v", t.ShardId, err)
		// remove incorrect data
		os.RemoveAll(t.DstStorePath)
		return err
	}

	t.succ()
	return nil
}

// restore restores a tar archive to the target path
func restoreFromTar(tarFile, path string) error {
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	os.MkdirAll(path, 0755)
	return tar.Restore(f, path)
}
