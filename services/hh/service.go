package hh // import "github.com/influxdata/influxdb/services/hh"

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"sync/atomic"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// ErrHintedHandoffDisabled is returned when attempting to use a
// disabled hinted handoff service.
var ErrHintedHandoffDisabled = fmt.Errorf("hinted handoff disabled")

const (
	writeShardReq       = "writeShardReq"
	writeShardReqPoints = "writeShardReqPoints"
)

// Service represents a hinted handoff service.
type Service struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closing chan struct{}

	processors map[uint64]*NodeProcessor

	Logger *zap.SugaredLogger
	cfg    Config

	shardWriter shardWriter
	MetaClient  metaClient

	Monitor interface {
		RegisterDiagnosticsClient(name string, client diagnostics.Client)
		DeregisterDiagnosticsClient(name string)
	}

	stats *HHStatistics
}

type shardWriter interface {
	WriteShard(shardID, ownerID uint64, points []models.Point) error
}

type metaClient interface {
	DataNode(id uint64) (ni *meta.NodeInfo, err error)
}

// NewService returns a new instance of Service.
func NewService(c Config, w shardWriter, m metaClient) *Service {
	//key := strings.Join([]string{"hh", c.Dir}, ":")
	//tags := map[string]string{"path": c.Dir}

	return &Service{
		cfg:         c,
		closing:     make(chan struct{}),
		processors:  make(map[uint64]*NodeProcessor),
		Logger:      zap.NewNop().Sugar(),
		shardWriter: w,
		MetaClient:  m,
		stats:       &HHStatistics{},
	}
}

func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "handoff")).Sugar()
}

// Open opens the hinted handoff service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cfg.Enabled {
		// Allow Open to proceed, but don't do anything.
		return nil
	}
	s.Logger.Info("Starting hinted handoff service")
	s.closing = make(chan struct{})

	// Register diagnostics if a Monitor service is available.
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("hh", s)
	}

	// Create the root directory if it doesn't already exist.
	s.Logger.Infof("Using data dir: %v", s.cfg.Dir)
	if err := os.MkdirAll(s.cfg.Dir, 0700); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Create a node processor for each node directory.
	files, err := ioutil.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		nodeID, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			// Not a number? Skip it.
			continue
		}

		n := NewNodeProcessor(nodeID, s.pathforNode(nodeID), s.shardWriter, s.MetaClient)
		if err := n.Open(); err != nil {
			return err
		}
		s.processors[nodeID] = n
	}

	s.wg.Add(1)
	go s.purgeInactiveProcessors()

	return nil
}

// Close closes the hinted handoff service.
func (s *Service) Close() error {
	s.Logger.Info("shutting down hh service")
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range s.processors {
		if err := p.Close(); err != nil {
			return err
		}
	}

	if s.Monitor != nil {
		s.Monitor.DeregisterDiagnosticsClient("hh")
	}

	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	s.closing = nil

	return nil
}

type HHStatistics struct {
	WriteShardReq       int64
	WriteShardReqPoints int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{{
		Name: "hinted_handoff",
		Tags: tags,
		Values: map[string]interface{}{
			writeShardReq:       atomic.LoadInt64(&s.stats.WriteShardReq),
			writeShardReqPoints: atomic.LoadInt64(&s.stats.WriteShardReqPoints),
		},
	}}

	for _, p := range s.processors {
		statistics = append(statistics, p.Statistics(tags)...)
	}
	return statistics
}

// WriteShard queues the points write for shardID to node ownerID to handoff queue
func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if !s.cfg.Enabled {
		return ErrHintedHandoffDisabled
	}
	atomic.AddInt64(&s.stats.WriteShardReq, 1)
	atomic.AddInt64(&s.stats.WriteShardReqPoints, int64(len(points)))

	s.mu.RLock()
	processor, ok := s.processors[ownerID]
	s.mu.RUnlock()
	if !ok {
		if err := func() error {
			// Check again under write-lock.
			s.mu.Lock()
			defer s.mu.Unlock()

			processor, ok = s.processors[ownerID]
			if !ok {
				processor = NewNodeProcessor(ownerID, s.pathforNode(ownerID), s.shardWriter, s.MetaClient)
				if err := processor.Open(); err != nil {
					return err
				}
				s.processors[ownerID] = processor
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	if err := processor.WriteShard(shardID, points); err != nil {
		return err
	}

	return nil
}

// Diagnostics returns diagnostic information.
func (s *Service) Diagnostics() (*diagnostics.Diagnostics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := &diagnostics.Diagnostics{
		Columns: []string{"node", "active", "last modified", "head", "tail"},
		Rows:    make([][]interface{}, 0, len(s.processors)),
	}

	for k, v := range s.processors {
		lm, err := v.LastModified()
		if err != nil {
			return nil, err
		}

		active, err := v.Active()
		if err != nil {
			return nil, err
		}

		d.Rows = append(d.Rows, []interface{}{k, active, lm, v.Head(), v.Tail()})
	}
	return d, nil
}

// purgeInactiveProcessors will cause the service to remove processors for inactive nodes.
func (s *Service) purgeInactiveProcessors() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Duration(s.cfg.PurgeInterval))
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()

				for k, v := range s.processors {
					lm, err := v.LastModified()
					if err != nil {
						s.Logger.Warnf("failed to determine LastModified for processor %d: %s", k, err.Error())
						continue
					}

					active, err := v.Active()
					if err != nil {
						s.Logger.Warnf("failed to determine if node %d is active: %s", k, err.Error())
						continue
					}
					if active {
						// Node is active.
						continue
					}

					if !lm.Before(time.Now().Add(-time.Duration(s.cfg.MaxAge))) {
						// Node processor contains too-young data.
						continue
					}

					if err := v.Close(); err != nil {
						s.Logger.Warnf("failed to close node processor %d: %s", k, err.Error())
						continue
					}
					if err := v.Purge(); err != nil {
						s.Logger.Warnf("failed to purge node processor %d: %s", k, err.Error())
						continue
					}
					delete(s.processors, k)
				}
			}()
		}
	}
}

// pathforNode returns the directory for HH data, for the given node.
func (s *Service) pathforNode(nodeID uint64) string {
	return filepath.Join(s.cfg.Dir, fmt.Sprintf("%d", nodeID))
}
