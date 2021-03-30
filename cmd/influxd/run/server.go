package run

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb"
	influxdb_coordinator "github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/control"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	"go.uber.org/zap"

	// Initialize the engine package
	_ "github.com/influxdata/influxdb/tsdb/engine"
	// Initialize the index package
	_ "github.com/influxdata/influxdb/tsdb/index"

	"github.com/angopher/chronus/coordinator"
	"github.com/angopher/chronus/services/controller"
	"github.com/angopher/chronus/services/hh"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/angopher/chronus/x"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string
	Listener    net.Listener

	Logger      *zap.Logger
	SugarLogger *zap.SugaredLogger

	Node              *influxdb.Node
	ClusterMetaClient *coordinator.ClusterMetaClient

	TSDBStore     *tsdb.Store
	QueryExecutor *query.Executor
	PointsWriter  *coordinator.PointsWriter
	ShardWriter   *coordinator.ShardWriter
	HintedHandoff *hh.Service
	Subscriber    *subscriber.Service

	Services []Service

	// These references are required for the tcp muxer.
	SnapshotterService *snapshotter.Service
	ClusterService     *coordinator.Service
	ControllerService  *controller.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	clusterExecutor *coordinator.ClusterExecutor

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config
}

// updateTLSConfig stores with into the tls config pointed at by into but only if with is not nil
// and into is nil. Think of it as setting the default value.
func updateTLSConfig(into **tls.Config, with *tls.Config) {
	if with != nil && into != nil && *into == nil {
		*into = with
	}
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo, logger *zap.Logger) (*Server, error) {
	// First grab the base tls config we will use for all clients and servers
	tlsConfig, err := c.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("tls configuration: %v", err)
	}

	// Update the TLS values on each of the configs to be the parsed one if
	// not already specified (set the default).
	updateTLSConfig(&c.HTTPD.TLS, tlsConfig)
	updateTLSConfig(&c.Subscriber.TLS, tlsConfig)
	for i := range c.OpenTSDBInputs {
		updateTLSConfig(&c.OpenTSDBInputs[i].TLS, tlsConfig)
	}

	// We need to ensure that a meta directory always exists even if
	// we don't start the meta store.  node.json is always stored under
	// the meta directory.
	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// 0.10-rc1 and prior would sometimes put the node.json at the root
	// dir which breaks backup/restore and restarting nodes.  This moves
	// the file from the root so it's always under the meta dir.
	oldPath := filepath.Join(filepath.Dir(c.Meta.Dir), "node.json")
	newPath := filepath.Join(c.Meta.Dir, "node.json")

	if _, err := os.Stat(oldPath); err == nil {
		if err := os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
	}

	node, err := influxdb.LoadNode(c.Meta.Dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		node = influxdb.NewNode(c.Meta.Dir)
	}

	var nodeID uint64 = 0
	if node != nil {
		nodeID = node.ID
	}

	if err := raftDBExists(c.Meta.Dir); err != nil {
		return nil, err
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Node: node,

		Logger:      logger,
		SugarLogger: logger.Sugar(),

		ClusterMetaClient: coordinator.NewMetaClient(c.Meta, c.Coordinator, nodeID),

		reportingDisabled: c.ReportingDisabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config: c,
	}
	s.ClusterMetaClient.WithLogger(s.Logger)
	s.Monitor = monitor.New(s, c.Monitor)
	s.config.registerDiagnostics(s.Monitor)

	if err := s.ClusterMetaClient.Open(); err != nil {
		return nil, err
	}
	s.ClusterMetaClient.Start()

	// If we've already created a data node for our id, we're done
	n, err := s.ClusterMetaClient.DataNode(nodeID)
	if err != nil {
		s.Logger.Warn(fmt.Sprintf("Node id(%d) from store can't be used, try to create new", nodeID), zap.Error(err))
		n, err = s.ClusterMetaClient.CreateDataNode(s.httpAPIAddr, s.tcpAddr)
		if err != nil {
			s.Logger.Warn(fmt.Sprint("Unable to create data node. err: ", err.Error()))
			return nil, err
		}
	}
	s.Node.ID = n.ID
	s.ClusterMetaClient.NodeID = n.ID
	if err := s.Node.Save(); err != nil {
		return nil, err
	}

	s.TSDBStore = tsdb.NewStore(c.Data.Dir)
	s.TSDBStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine
	s.TSDBStore.EngineOptions.IndexVersion = c.Data.Index

	// Create the Subscriber service
	s.Subscriber = subscriber.NewService(c.Subscriber)

	// Initialize shard writer
	s.ShardWriter = coordinator.NewShardWriter(
		time.Duration(s.config.Coordinator.WriteTimeout),
		coordinator.NewClientPool(func(nodeId uint64) (x.ConnPool, error) {
			return x.NewBoundedPool(
				1,
				100,
				time.Duration(s.config.Coordinator.PoolMaxIdleTimeout),
				time.Duration(s.config.Coordinator.DailTimeout),
				coordinator.NewClientConnFactory(
					nodeId,
					time.Duration(s.config.Coordinator.DailTimeout),
					s.ClusterMetaClient,
				).Dial,
			)
		}),
	)
	s.ShardWriter.WithLogger(s.Logger)

	// Create the hinted handoff service
	s.HintedHandoff = hh.NewService(c.HintedHandoff, s.ShardWriter, s.ClusterMetaClient)
	s.HintedHandoff.Monitor = s.Monitor
	s.HintedHandoff.WithLogger(s.Logger)

	// Initialize points writer.
	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.WriteTimeout = time.Duration(c.Coordinator.WriteTimeout)
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.ShardWriter = s.ShardWriter
	s.PointsWriter.Node = s.Node
	s.PointsWriter.HintedHandoff = s.HintedHandoff

	// Initialize cluster extecutor
	clusterExecutor := coordinator.NewClusterExecutor(
		s.Node, s.TSDBStore,
		s.ClusterMetaClient,
		coordinator.NewClientPool(func(nodeId uint64) (x.ConnPool, error) {
			return x.NewBoundedPool(
				x.Max(1, s.config.Coordinator.PoolMinStreamsPerNode),
				s.config.Coordinator.PoolMaxStreamsPerNode,
				time.Duration(s.config.Coordinator.PoolMaxIdleTimeout),
				time.Duration(s.config.Coordinator.DailTimeout),
				coordinator.NewClientConnFactory(
					nodeId,
					time.Duration(s.config.Coordinator.DailTimeout),
					s.ClusterMetaClient,
				).Dial,
			)
		}),
		s.config.Coordinator,
	)
	clusterExecutor.WithLogger(s.Logger)

	// Initialize query executor.
	s.clusterExecutor = clusterExecutor
	s.QueryExecutor = query.NewExecutor()
	s.QueryExecutor.StatementExecutor = &influxdb_coordinator.StatementExecutor{
		MetaClient:  s.ClusterMetaClient,
		TaskManager: clusterExecutor,
		TSDBStore:   clusterExecutor,
		ShardMapper: &coordinator.ClusterShardMapper{
			MetaClient:      s.ClusterMetaClient,
			Node:            s.Node,
			ClusterExecutor: clusterExecutor,
		},
		Monitor:           s.Monitor,
		PointsWriter:      s.PointsWriter,
		MaxSelectPointN:   c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:  c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN: c.Coordinator.MaxSelectBucketsN,
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries

	clusterExecutor.TaskManager = s.QueryExecutor.TaskManager

	// Initialize the monitor
	s.Monitor.Version = s.buildInfo.Version
	s.Monitor.Commit = s.buildInfo.Commit
	s.Monitor.Branch = s.buildInfo.Branch
	s.Monitor.BuildTime = s.buildInfo.Time
	s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s, nil
}

// Statistics returns statistics for the services running in the Server.
func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.QueryExecutor.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.Subscriber.Statistics(tags)...)
	for _, srv := range s.Services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	return statistics
}

func (s *Server) appendClusterService(c coordinator.Config) {
	srv := coordinator.NewService(c)
	srv.TaskManager = s.QueryExecutor.TaskManager
	srv.TSDBStore = s.TSDBStore
	srv.Node = s.Node
	srv.MetaClient = s.ClusterMetaClient
	srv.WithLogger(s.Logger)
	s.Services = append(s.Services, srv)
	s.ClusterService = srv
}

func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.ClusterMetaClient
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) {
	s.Logger = logger.New(w)
}

func (s *Server) appendMonitorService() {
	s.Services = append(s.Services, s.Monitor)
}

func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaClient = s.ClusterMetaClient
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c)
	srv.Handler.MetaClient = s.ClusterMetaClient
	authorizer := &imeta.Authorizer{}
	srv.Handler.QueryAuthorizer = authorizer
	srv.Handler.WriteAuthorizer = authorizer
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.Monitor = s.Monitor
	srv.Handler.PointsWriter = s.PointsWriter
	srv.Handler.Version = s.buildInfo.Version
	srv.Handler.BuildType = "OSS"
	ss := storage.NewStore(s.TSDBStore, s.ClusterMetaClient)
	srv.Handler.Store = ss
	srv.Handler.Controller = control.NewController(s.ClusterMetaClient, reads.NewReader(ss), authorizer, c.AuthEnabled, s.Logger)

	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	srv.MetaClient = s.ClusterMetaClient
	srv.PointsWriter = s.PointsWriter
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.ClusterMetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}

	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.ClusterMetaClient
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendPrecreatorService(c precreator.Config) error {
	if !c.Enabled {
		return nil
	}
	srv := precreator.NewService(c)
	srv.MetaClient = s.ClusterMetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.ClusterMetaClient
	s.Services = append(s.Services, srv)
}

func (s *Server) appendContinuousQueryService(c continuous_querier.Config) {
	if !c.Enabled {
		return
	}
	srv := continuous_querier.NewService(c)
	srv.MetaClient = s.ClusterMetaClient
	srv.QueryExecutor = s.QueryExecutor
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
}

func (s *Server) appendControllerService(c controller.Config) {
	if !c.Enabled {
		return
	}
	srv := controller.NewService(c)
	srv.MetaClient = s.ClusterMetaClient
	srv.Node = s.Node
	srv.TSDBStore = s.TSDBStore

	s.ControllerService = srv
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling, if set.
	startProfile(s.CPUProfile, s.MemProfile)

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	mux := tcp.NewMux()
	go mux.Serve(ln)

	// Append services.
	s.appendMonitorService()
	s.appendPrecreatorService(s.config.Precreator)
	s.appendClusterService(s.config.Coordinator)
	s.appendSnapshotterService()
	s.appendContinuousQueryService(s.config.ContinuousQuery)
	s.appendHTTPDService(s.config.HTTPD)
	s.appendRetentionPolicyService(s.config.Retention)
	s.appendControllerService(s.config.Controller)
	for _, i := range s.config.GraphiteInputs {
		if err := s.appendGraphiteService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.CollectdInputs {
		s.appendCollectdService(i)
	}
	for _, i := range s.config.OpenTSDBInputs {
		if err := s.appendOpenTSDBService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.UDPInputs {
		s.appendUDPService(i)
	}

	s.Subscriber.MetaClient = s.ClusterMetaClient
	s.PointsWriter.MetaClient = s.ClusterMetaClient
	s.ShardWriter.MetaClient = s.ClusterMetaClient
	s.Monitor.MetaClient = s.ClusterMetaClient

	s.ClusterService.Listener = mux.Listen(coordinator.MuxHeader)
	s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)
	s.ControllerService.Listener = mux.Listen(controller.MuxHeader)

	// Configure logging for all services and clients.
	s.TSDBStore.WithLogger(s.Logger)
	if s.config.Data.QueryLogEnabled {
		s.QueryExecutor.WithLogger(s.Logger)
	}
	s.PointsWriter.WithLogger(s.Logger)
	s.Subscriber.WithLogger(s.Logger)
	for _, svc := range s.Services {
		svc.WithLogger(s.Logger)
	}
	s.SnapshotterService.WithLogger(s.Logger)
	s.Monitor.WithLogger(s.Logger)

	// Open TSDB store.
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the subscriber service
	if err := s.Subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	// Open the hinted handoff service
	if err := s.HintedHandoff.Open(); err != nil {
		return fmt.Errorf("open hinted handoff: %s", err)
	}

	// Open the points writer service
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	//TODO:
	//s.PointsWriter.AddWriteSubscriber(s.Subscriber.Points())

	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
		}
	}

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}

	go s.poolStatsLoop()

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	s.config.deregisterDiagnostics(s.Monitor)

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.QueryExecutor != nil {
		s.QueryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

// reportServer reports usage statistics about the system.
func (s *Server) reportServer() {
	dbs := s.ClusterMetaClient.Databases()
	numDatabases := len(dbs)

	var (
		numMeasurements int64
		numSeries       int64
	)

	for _, db := range dbs {
		name := db.Name
		n, err := s.TSDBStore.SeriesCardinality(name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get series cardinality for database %s: %v", name, err))
		} else {
			numSeries += n
		}

		n, err = s.TSDBStore.MeasurementsCardinality(name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get measurement cardinality for database %s: %v", name, err))
		} else {
			numMeasurements += n
		}
	}

	clusterID := s.ClusterMetaClient.ClusterID()
	cl := client.New("")
	usage := client.Usage{
		Product: "influxdb",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	s.Logger.Info("Sending usage statistics to usage.influxdata.com")

	go cl.Save(usage)
}

func dumpPoolStats(name string, sugar *zap.SugaredLogger, stats []coordinator.StatEntity) {
	sugar.Info("Dump statistics of ", name)
	sugar.Info("active/idle/capacity, success/cost(ms), failure/cost(ms), return/close")
	for _, stat := range stats {
		sugar.Infof("Node %d: %d/%d/%d, %d/%d, %d/%d, %d/%d",
			stat.NodeId,
			stat.Stat.Active, stat.Stat.Idle, stat.Stat.Capacity,
			stat.Stat.GetSuccessCnt, stat.Stat.GetSuccessMillis,
			stat.Stat.GetFailureCnt, stat.Stat.GetFailureMillis,
			stat.Stat.ReturnCnt, stat.Stat.CloseCnt,
		)
	}
}

func (s *Server) poolStatsLoop() {
	ticker := time.NewTicker(120 * time.Second)

LOOP:
	for {
		select {
		case <-ticker.C:
			dumpPoolStats("remote executors", s.SugarLogger, s.clusterExecutor.RemoteNodeExecutor.Stats())
			dumpPoolStats("shard writers", s.SugarLogger, s.ShardWriter.Stats())
		case <-s.closing:
			//shutdown
			break LOOP
		}
	}
}

// Service represents a service attached to the server.
type Service interface {
	WithLogger(log *zap.Logger)
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

// monitorPointsWriter is a wrapper around `coordinator.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter coordinator.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	return (*coordinator.PointsWriter)(pw).WritePointsPrivileged(database, retentionPolicy, models.ConsistencyLevelAny, points)
}

func raftDBExists(dir string) error {
	// Check to see if there is a raft db, if so, error out with a message
	// to downgrade, export, and then import the meta data
	raftFile := filepath.Join(dir, "raft.db")
	if _, err := os.Stat(raftFile); err == nil {
		return fmt.Errorf("detected %s. To proceed, you'll need to either 1) downgrade to v0.11.x, export your metadata, upgrade to the current version again, and then import the metadata or 2) delete the file, which will effectively reset your database. For more assistance with the upgrade, see: https://docs.influxdata.com/influxdb/v0.12/administration/upgrading/", raftFile)
	}
	return nil
}
