package coordinator

import (
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb/services/meta"
)

// Service and remote executor are designed for "forwarded" requests
//	between shards/nodes. For better performance we separate the design
//	into several phases:
//	1. Introduce connection pooling avoiding create/destroy connections frequently.
//	2. Introduce multiplexing to reduce the cost of connections further more.
//	But we should also notice that more than one connection are needed to avoid flow control of TCP.
// Now we implement connection pooling first and keep an versioning api which enables
//	data node declaring its running version.

// MaxMessageSize defines how large a message can be before we reject it
const MaxMessageSize = 1024 * 1024 * 1024 // 1GB

// MuxHeader is the header byte used in the TCP mux.
const MuxHeader = 2

// Statistics maintained by the cluster package
const (
	writeShardReq       = "writeShardReq"
	writeShardPointsReq = "writeShardPointsReq"
	writeShardFail      = "writeShardFail"

	createIteratorReq  = "createIteratorReq"
	createIteratorFail = "createIteratorFail"

	fieldDimensionsReq  = "fieldDimensionsReq"
	fieldDimensionsFail = "fieldDimensionsFail"

	tagKeysReq  = "tagKeysReq"
	tagKeysFail = "tagKeysFail"

	tagValuesReq  = "tagValuesReq"
	tagValuesFail = "tagValuesFail"

	measurementNamesReq  = "measurementNamesReq"
	measurementNamesFail = "measurementNamesFail"

	seriesCardinalityReq  = "seriesCardinalityReq"
	seriesCardinalityFail = "seriesCardinalityFail"

	iteratorCostReq  = "iteratorCostReq"
	iteratorCostFail = "iteratorCostFail"

	mapTypeReq  = "mapTypeReq"
	mapTypeFail = "mapTypeFail"
)

type ServerResponse interface {
	SetCode(int)
	SetMessage(string)
	MarshalBinary() ([]byte, error)
}

// Service processes data received over raw TCP connections.
type Service struct {
	mu      sync.RWMutex
	Node    *influxdb.Node
	wg      sync.WaitGroup
	closing chan struct{}

	Listener net.Listener

	MetaClient interface {
		ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo)
	}

	TSDBStore   TSDBStore
	TaskManager *query.TaskManager

	Logger *zap.Logger
	stats  *InternalServiceStatistics
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		closing: make(chan struct{}),
		//Logger:  log.New(os.Stderr, "[cluster] ", log.LstdFlags),
		stats:  &InternalServiceStatistics{},
		Logger: zap.NewNop(),
	}
}

// Open opens the network listener and begins serving requests.
func (s *Service) Open() error {

	s.Logger.Info("Starting cluster service")
	// Begin serving conections.
	s.wg.Add(1)
	go s.serve()

	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "cluster"))
}

type InternalServiceStatistics struct {
	WriteShardReq       int64
	WriteShardPointsReq int64
	WriteShardFail      int64

	CreateIteratorReq  int64
	CreateIteratorFail int64

	FieldDimensionsReq  int64
	FieldDimensionsFail int64

	TagKeysReq  int64
	TagKeysFail int64

	TagValuesReq  int64
	TagValuesFail int64

	MeasurementNamesReq  int64
	MeasurementNamesFail int64

	SeriesCardinalityReq  int64
	SeriesCardinalityFail int64

	IteratorCostReq  int64
	IteratorCostFail int64

	MapTypeReq  int64
	MapTypeFail int64
}

func (w *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "coordinator_service",
		Tags: tags,
		Values: map[string]interface{}{
			writeShardReq:       atomic.LoadInt64(&w.stats.WriteShardReq),
			writeShardPointsReq: atomic.LoadInt64(&w.stats.WriteShardPointsReq),
			writeShardFail:      atomic.LoadInt64(&w.stats.WriteShardFail),

			createIteratorReq:  atomic.LoadInt64(&w.stats.CreateIteratorReq),
			createIteratorFail: atomic.LoadInt64(&w.stats.CreateIteratorFail),

			fieldDimensionsReq:  atomic.LoadInt64(&w.stats.FieldDimensionsReq),
			fieldDimensionsFail: atomic.LoadInt64(&w.stats.FieldDimensionsFail),

			tagKeysReq:  atomic.LoadInt64(&w.stats.TagKeysReq),
			tagKeysFail: atomic.LoadInt64(&w.stats.TagKeysFail),

			tagValuesReq:  atomic.LoadInt64(&w.stats.TagValuesReq),
			tagValuesFail: atomic.LoadInt64(&w.stats.TagValuesFail),

			measurementNamesReq:  atomic.LoadInt64(&w.stats.MeasurementNamesReq),
			measurementNamesFail: atomic.LoadInt64(&w.stats.MeasurementNamesFail),

			seriesCardinalityReq:  atomic.LoadInt64(&w.stats.SeriesCardinalityReq),
			seriesCardinalityFail: atomic.LoadInt64(&w.stats.SeriesCardinalityFail),

			iteratorCostReq:  atomic.LoadInt64(&w.stats.IteratorCostReq),
			iteratorCostFail: atomic.LoadInt64(&w.stats.IteratorCostFail),

			mapTypeReq:  atomic.LoadInt64(&w.stats.MapTypeReq),
			mapTypeFail: atomic.LoadInt64(&w.stats.MapTypeFail),
		},
	}}
}

// serve accepts connections from the listener and handles them.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down.
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection.
		conn, err := s.Listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "connection closed") {
				s.Logger.Error("cluster service accept fail", zap.Error(err))
				return
			}
			s.Logger.Error("accept error", zap.Error(err))
			continue
		}

		// Delegate connection handling to a separate goroutine.
		s.wg.Add(1)
		go func() {
			defer func() {
				s.wg.Done()
				if err := recover(); err != nil {
					buf := debug.Stack()
					s.Logger.Error("recover from panic", zap.String("stack", string(buf)))
				}
			}()

			s.handleConn(conn)
		}()
	}
}

// Close shuts down the listener and waits for all connections to finish.
func (s *Service) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Shut down all handlers.
	close(s.closing)
	s.wg.Wait()

	return nil
}

func (s *Service) handle(conn net.Conn, typ byte, data []byte) (respType byte, resp encoding.BinaryMarshaler, err error) {
	// Delegate message processing by type.
	switch typ {
	case writeShardRequestMessage:
		err = s.processWriteShardRequest(data)
		respType = writeShardResponseMessage
		resp = &WriteShardResponse{}
	case executeStatementRequestMessage:
		err = s.processExecuteStatementRequest(data)
		respType = writeShardResponseMessage
		resp = &WriteShardResponse{}
	case createIteratorRequestMessage:
		s.processCreateIteratorRequest(conn, data)
	case fieldDimensionsRequestMessage:
		respType = fieldDimensionsResponseMessage
		resp, err = s.processFieldDimensionsRequest(data)
	case tagKeysRequestMessage:
		respType = tagKeysResponseMessage
		resp, err = s.processTagKeysRequest(data)
	case tagValuesRequestMessage:
		respType = tagValuesResponseMessage
		resp, err = s.processTagValuesRequest(data)
	case measurementNamesRequestMessage:
		respType = measurementNamesResponseMessage
		resp, err = s.processMeasurementNamesRequest(data)
	case seriesCardinalityRequestMessage:
		respType = seriesCardinalityResponseMessage
		resp, err = s.processSeriesCardinalityRequest(data)
	case deleteSeriesRequestMessage:
		respType = deleteSeriesResponseMessage
		resp, err = s.processDeleteSeriesRequest(data)
	case deleteDatabaseRequestMessage:
		respType = deleteDatabaseResponseMessage
		resp, err = s.processDeleteDatabaseRequest(data)
	case deleteMeasurementRequestMessage:
		respType = deleteMeasurementResponseMessage
		resp, err = s.processDeleteMeasurementRequest(data)
	case iteratorCostRequestMessage:
		respType = iteratorCostResponseMessage
		resp, err = s.processIteratorCostRequest(data)
	case mapTypeRequestMessage:
		respType = mapTypeResponseMessage
		resp, err = s.processMapTypeRequest(data)
	case executeTaskManagerRequestMessage:
		respType = executeTaskManagerResponseMessage
		resp, err = s.processTaskManagerRequest(data)
	case testRequestMessage:
		// do nothing
	default:
		s.Logger.Error("cluster service message type not found", zap.Uint8("type", typ))
	}
	if err != nil {
		s.Logger.Error("process request error", zap.Uint8("type", typ), zap.Error(err))
	}
	if resp == nil {
		return
	}
	if serverResp, ok := resp.(ServerResponse); ok {
		if err != nil {
			serverResp.SetCode(1)
			serverResp.SetMessage(err.Error())
		} else {
			serverResp.SetCode(0)
		}
	}
	return
}

func (s *Service) writeClusterResponse(conn net.Conn, respType byte, resp encoding.BinaryMarshaler) (err error) {
	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Error("error marshalling response", zap.Uint8("respType", respType), zap.Error(err))
		return
	}

	// Write to connection.
	if err = WriteTLV(conn, respType, buf); err != nil {
		s.Logger.Error("WriteTLV fail", zap.Error(err))
	}
	return
}

// handleConn services an individual TCP connection.
func (s *Service) handleConn(conn net.Conn) {
	// Ensure connection is closed when service is closed.
	defer func() {
		defer conn.Close()
		s.Logger.Info(fmt.Sprintf("close remote connection from %+v", conn.RemoteAddr()))
	}()
	s.Logger.Info(fmt.Sprintf("accept remote connection from %+v", conn.RemoteAddr()))
	for {
		select {
		case <-s.closing:
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		typ, data, err := ReadTLV(conn)
		conn.SetDeadline(time.Time{})
		if err == io.EOF {
			return
		}
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		}
		if err != nil {
			s.Logger.Error("read error", zap.Error(err))
			break
		}
		if typ == 0 {
			continue
		}
		respType, resp, err := s.handle(conn, typ, data)
		if resp == nil {
			continue
		}
		err = s.writeClusterResponse(conn, respType, resp)
		if err != nil {
			// close conn due to error in writing
			break
		}
	}
}

func (s *Service) processTaskManagerRequest(buf []byte) (*TaskManagerStatementResponse, error) {
	var (
		resp TaskManagerStatementResponse
		err  error
	)
	if err = func() error {
		var req TaskManagerStatementRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		stmt, err := influxql.ParseStatement(req.Statement())
		if err != nil {
			return err
		}

		recvCtx := &query.ExecutionContext{
			Context: context.Background(),
			Results: make(chan *query.Result, 1),
		}
		err = s.TaskManager.ExecuteStatement(stmt, recvCtx)
		if err != nil {
			return err
		}
		resp.Result = *(<-recvCtx.Results)
		return nil
	}(); err != nil {
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processMapTypeRequest(buf []byte) (*MapTypeResponse, error) {
	var (
		resp MapTypeResponse
		err  error
	)
	atomic.AddInt64(&s.stats.MapTypeReq, 1)
	if err = func() error {
		var req MapTypeRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		if len(req.Sources) == 0 {
			return errors.New(fmt.Sprintf("bad request %+v: no sources", req))
		}
		m := req.Sources[0].(*influxql.Measurement)

		sg := s.TSDBStore.ShardGroup(req.ShardIDs)

		var names []string
		if m.Regex != nil && m.Name != "_series" && m.Name != "_fieldKeys" && m.Name != "_tagKeys" {
			names = sg.MeasurementsByRegex(m.Regex.Val)
		} else {
			names = []string{m.Name}
		}

		typ := influxql.Unknown
		for _, name := range names {
			if m.SystemIterator != "" {
				name = m.SystemIterator
			}
			t := sg.MapType(name, req.Field)
			if typ.LessThan(t) {
				typ = t
			}
		}
		resp.DataType = typ
		return nil
	}(); err != nil {
		atomic.AddInt64(&s.stats.MapTypeFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processIteratorCostRequest(buf []byte) (*IteratorCostResponse, error) {
	var (
		resp IteratorCostResponse
		err  error
	)
	atomic.AddInt64(&s.stats.IteratorCostReq, 1)
	if err = func() error {
		var req IteratorCostRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		if len(req.Sources) == 0 {
			return errors.New(fmt.Sprintf("bad request %+v: no sources", req))
		}
		m := req.Sources[0].(*influxql.Measurement)
		opt := req.Opt

		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if m.Regex != nil {
			resp.Cost, err = func() (query.IteratorCost, error) {
				var costs query.IteratorCost
				measurements := sg.MeasurementsByRegex(m.Regex.Val)
				for _, measurement := range measurements {
					c, err := sg.IteratorCost(measurement, opt)
					if err != nil {
						return c, err
					}
					costs = costs.Combine(c)
				}
				return costs, nil
			}()
		} else {
			resp.Cost, err = sg.IteratorCost(m.Name, opt)
		}
		return err
	}(); err != nil {
		atomic.AddInt64(&s.stats.IteratorCostFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processDeleteMeasurementRequest(buf []byte) (*DeleteMeasurementResponse, error) {
	var (
		resp DeleteMeasurementResponse
		err  error
	)
	if err = func() error {
		var req DeleteMeasurementRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}
		return s.TSDBStore.DeleteMeasurement(req.Database, req.Name)
	}(); err != nil {
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processDeleteDatabaseRequest(buf []byte) (*DeleteDatabaseResponse, error) {
	var (
		resp DeleteDatabaseResponse
		err  error
	)
	if err = func() error {
		var req DeleteDatabaseRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}
		return s.TSDBStore.DeleteDatabase(req.Database)
	}(); err != nil {
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processDeleteSeriesRequest(buf []byte) (*DeleteSeriesResponse, error) {
	var (
		resp DeleteSeriesResponse
		err  error
	)
	if err = func() error {
		var req DeleteSeriesRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		cond := influxql.Reduce(req.Cond, &influxql.NowValuer{Now: time.Now().UTC()})
		err := s.TSDBStore.DeleteSeries(req.Database, req.Sources, cond)
		return err
	}(); err != nil {
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processSeriesCardinalityRequest(buf []byte) (*SeriesCardinalityResponse, error) {
	var (
		resp SeriesCardinalityResponse
		err  error
	)
	atomic.AddInt64(&s.stats.SeriesCardinalityReq, 1)
	if err = func() error {
		var req SeriesCardinalityRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		n, err := s.TSDBStore.SeriesCardinality(req.Database)
		resp.N = n
		return err
	}(); err != nil {
		atomic.AddInt64(&s.stats.SeriesCardinalityFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processMeasurementNamesRequest(buf []byte) (*MeasurementNamesResponse, error) {
	var (
		resp MeasurementNamesResponse
		err  error
	)
	if err = func() error {
		var req MeasurementNamesRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		var err error
		resp.Names, err = s.TSDBStore.MeasurementNames(nil, req.Database, req.Cond)
		return err
	}(); err != nil {
		atomic.AddInt64(&s.stats.MeasurementNamesFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processTagKeysRequest(buf []byte) (*TagKeysResponse, error) {
	var (
		resp TagKeysResponse
		err  error
	)
	atomic.AddInt64(&s.stats.TagKeysReq, 1)
	if err = func() error {
		var req TagKeysRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		var err error
		resp.TagKeys, err = s.TSDBStore.TagKeys(nil, req.ShardIDs, req.Cond)
		return err
	}(); err != nil {
		atomic.AddInt64(&s.stats.TagKeysFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processTagValuesRequest(buf []byte) (*TagValuesResponse, error) {
	var (
		resp TagValuesResponse
		err  error
	)
	atomic.AddInt64(&s.stats.TagValuesReq, 1)
	if err = func() error {
		var req TagValuesRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		var err error
		resp.TagValues, err = s.TSDBStore.TagValues(nil, req.ShardIDs, req.Cond)
		return err
	}(); err != nil {
		atomic.AddInt64(&s.stats.TagValuesFail, 1)
		resp.Err = err.Error()
	}
	return &resp, err
}

func (s *Service) processExecuteStatementRequest(buf []byte) error {
	// Unmarshal the request.
	var req ExecuteStatementRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Parse the InfluxQL statement.
	stmt, err := influxql.ParseStatement(req.Statement())
	if err != nil {
		return err
	}

	return s.executeStatement(stmt, req.Database())
}

func (s *Service) executeStatement(stmt influxql.Statement, database string) error {
	switch t := stmt.(type) {
	case *influxql.DropDatabaseStatement:
		return s.TSDBStore.DeleteDatabase(t.Name)
	case *influxql.DropMeasurementStatement:
		return s.TSDBStore.DeleteMeasurement(database, t.Name)
	case *influxql.DropSeriesStatement:
		return s.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
	case *influxql.DropRetentionPolicyStatement:
		return s.TSDBStore.DeleteRetentionPolicy(database, t.Name)
	default:
		return fmt.Errorf("%q should not be executed across a cluster", stmt.String())
	}
}

func (s *Service) processWriteShardRequest(buf []byte) error {
	// Build request
	var req WriteShardRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	points := req.Points()
	// stats
	atomic.AddInt64(&s.stats.WriteShardReq, 1)
	atomic.AddInt64(&s.stats.WriteShardPointsReq, int64(len(points)))
	err := s.TSDBStore.WriteToShard(req.ShardID(), points)

	// We may have received a write for a shard that we don't have locally because the
	// sending node may have just created the shard (via the metastore) and the write
	// arrived before the local store could create the shard.  In this case, we need
	// to check the metastore to determine what database and retention policy this
	// shard should reside within.
	if err == tsdb.ErrShardNotFound {
		db, rp := req.Database(), req.RetentionPolicy()
		if db == "" || rp == "" {
			s.Logger.Error("drop write request: no database or rentention policy received\n",
				zap.Uint64("shard", req.ShardID()))
			return nil
		}

		err = s.TSDBStore.CreateShard(req.Database(), req.RetentionPolicy(), req.ShardID(), true) //enable what mean?
		if err != nil {
			atomic.AddInt64(&s.stats.WriteShardFail, 1)
			return fmt.Errorf("create shard %d: %s", req.ShardID(), err)
		}

		err = s.TSDBStore.WriteToShard(req.ShardID(), points)
		if err != nil {
			atomic.AddInt64(&s.stats.WriteShardFail, 1)
			return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
		}
	}

	if err != nil {
		atomic.AddInt64(&s.stats.WriteShardFail, 1)
		return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
	}

	return nil
}

func (s *Service) processCreateIteratorRequest(conn net.Conn, buf []byte) {
	var itr query.Iterator
	var trace *tracing.Trace
	var span *tracing.Span
	ioError := false
	defer func() {
		if ioError {
			conn.Close()
		}
	}()
	respType := createIteratorResponseMessage
	if err := func() error {
		// Parse request.
		var req CreateIteratorRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		ctx := context.Background()
		if req.SpanContex != nil {
			trace, span = tracing.NewTraceFromSpan(fmt.Sprintf("remote_node_id: %d", s.Node.ID), *req.SpanContex)
			ctx = tracing.NewContextWithTrace(ctx, trace)
			ctx = tracing.NewContextWithSpan(ctx, span)
			//var aux query.Iterators
			//ctx = query.NewContextWithIterators(ctx, &aux)
		}

		var err error

		//TODO:求证是否直接使用&req.Measurement会不会导致内存错误
		m := new(influxql.Measurement)
		*m = req.Measurement

		sg := s.TSDBStore.ShardGroup(req.ShardIDs)
		if m.Regex != nil {
			measurements := sg.MeasurementsByRegex(m.Regex.Val)
			inputs := make([]query.Iterator, 0, len(measurements))
			if err := func() error {
				for _, measurement := range measurements {
					mm := m.Clone()
					mm.Name = measurement
					input, err := sg.CreateIterator(ctx, mm, req.Opt)
					if err != nil {
						return err
					}
					inputs = append(inputs, input)
				}
				return nil
			}(); err != nil {
				query.Iterators(inputs).Close()
				return err
			}

			itr, err = query.Iterators(inputs).Merge(req.Opt)
		} else {
			itr, err = sg.CreateIterator(ctx, m, req.Opt)
		}

		if err != nil {
			return err
		}
		// Generate a single iterator from all shards.
		//i, err := influxql.IteratorCreators(ics).CreateIterator(req.Opt)

		return nil
	}(); err != nil {
		atomic.AddInt64(&s.stats.CreateIteratorFail, 1)
		if itr != nil {
			itr.Close()
		}
		s.Logger.Error("error reading CreateIterator request fail", zap.Error(err))
		if err = EncodeTLV(conn, respType, &CreateIteratorResponse{Err: err}); err != nil {
			s.Logger.Error("CreateIteratorRequest EncodeTLV fail", zap.Error(err))
			ioError = true
		}
		return
	}

	dataType := influxql.Unknown
	switch itr.(type) {
	case query.FloatIterator:
		dataType = influxql.Float
	case query.IntegerIterator:
		dataType = influxql.Integer
	case query.StringIterator:
		dataType = influxql.String
	case query.BooleanIterator:
		dataType = influxql.Boolean
	}

	seriesN := 0
	if itr != nil {
		seriesN = itr.Stats().SeriesN
	}
	// Encode success response.
	itrTerminator := x.RandBytes(8)
	if err := EncodeTLV(conn, respType, &CreateIteratorResponse{DataType: dataType, SeriesN: seriesN, Termination: itrTerminator}); err != nil {
		s.Logger.Error("error writing CreateIterator response, EncodeTLV fail", zap.Error(err))
		atomic.AddInt64(&s.stats.CreateIteratorFail, 1)
		ioError = true
		return
	}

	// Exit if no iterator was produced.
	if itr == nil {
		return
	}

	// Stream iterator to connection.
	encoder := query.NewIteratorEncoder(conn)
	if err := encoder.EncodeIterator(itr); err != nil {
		s.Logger.Error("encoding CreateIterator iterator fail", zap.Error(err))
		atomic.AddInt64(&s.stats.CreateIteratorFail, 1)
		ioError = true
		return
	}

	itr.Close()

	if trace != nil {
		span.Finish()
		if err := encoder.EncodeTrace(trace); err != nil {
			s.Logger.Error("EncodeTrace fail", zap.Error(err))
			atomic.AddInt64(&s.stats.CreateIteratorFail, 1)
			ioError = true
			return
		}
	}

	// Write termination of iterator
	if _, err := conn.Write(itrTerminator); err != nil {
		ioError = true
	}
}

func (s *Service) processFieldDimensionsRequest(buf []byte) (*FieldDimensionsResponse, error) {
	var (
		err error
	)
	var fields map[string]influxql.DataType
	var dimensions map[string]struct{}
	atomic.AddInt64(&s.stats.FieldDimensionsReq, 1)
	if err = func() error {
		// Parse request.
		var req FieldDimensionsRequest
		if err := req.UnmarshalBinary(buf); err != nil {
			return err
		}

		// Generate a single iterator from all shards.
		measurements := make([]string, 0)
		ms := req.Sources.Measurements()
		for _, m := range ms {
			if m.Regex != nil {
				measurements = s.TSDBStore.ShardGroup(req.ShardIDs).MeasurementsByRegex(m.Regex.Val)
			} else {
				measurements = append(measurements, m.Name)
			}
		}

		f, d, err := s.TSDBStore.ShardGroup(req.ShardIDs).FieldDimensions(measurements)
		if err != nil {
			return err
		}
		fields, dimensions = f, d

		return nil
	}(); err != nil {
		atomic.AddInt64(&s.stats.FieldDimensionsFail, 1)
		return &FieldDimensionsResponse{Err: err}, err
	}
	return &FieldDimensionsResponse{
		Fields:     fields,
		Dimensions: dimensions,
	}, err
}

// ReadTLV reads a type-length-value record from r.
func ReadTLV(r io.Reader) (byte, []byte, error) {
	typ, err := ReadType(r)
	if err != nil {
		return 0, nil, err
	}

	buf, err := ReadLV(r)
	if err != nil {
		return 0, nil, err
	}
	return typ, buf, err
}

// ReadType reads the type from a TLV record.
func ReadType(r io.Reader) (byte, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return 0, err
	}
	return typ[0], nil
}

// ReadLV reads the length-value from a TLV record.
func ReadLV(r io.Reader) ([]byte, error) {
	// Read the size of the message.
	var sz int64
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return nil, err
	}

	if sz >= MaxMessageSize {
		return nil, fmt.Errorf("max message size of %d exceeded: %d", MaxMessageSize, sz)
	}

	if sz == 0 {
		// empty msg
		return []byte{}, nil
	}

	// Read the value.
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// WriteTLV writes a type-length-value record to w.
func WriteTLV(w io.Writer, typ byte, buf []byte) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}

// WriteType writes the type in a TLV record to w.
func WriteType(w io.Writer, typ byte) error {
	if _, err := w.Write([]byte{typ}); err != nil {
		return err
	}
	return nil
}

// WriteLV writes the length-value in a TLV record to w.
func WriteLV(w io.Writer, buf []byte) error {
	// Write the size of the message.
	if err := binary.Write(w, binary.BigEndian, int64(len(buf))); err != nil {
		return err
	}

	// Write the value.
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

// EncodeTLV encodes v to a binary format and writes the record-length-value record to w.
func EncodeTLV(w io.Writer, typ byte, v encoding.BinaryMarshaler) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := EncodeLV(w, v); err != nil {
		return err
	}
	return nil
}

// EncodeLV encodes v to a binary format and writes the length-value record to w.
func EncodeLV(w io.Writer, v encoding.BinaryMarshaler) error {
	buf, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}

// DecodeTLV reads the type-length-value record from r and unmarshals it into v.
func DecodeTLV(r io.Reader, v encoding.BinaryUnmarshaler) (typ byte, err error) {
	typ, err = ReadType(r)
	if err != nil {
		return 0, err
	}
	if err := DecodeLV(r, v); err != nil {
		return 0, err
	}
	return typ, nil
}

// DecodeLV reads the length-value record from r and unmarshals it into v.
func DecodeLV(r io.Reader, v encoding.BinaryUnmarshaler) error {
	buf, err := ReadLV(r)
	if err != nil {
		return err
	}

	if err := v.UnmarshalBinary(buf); err != nil {
		return err
	}
	return nil
}
