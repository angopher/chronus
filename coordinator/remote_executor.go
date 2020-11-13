package coordinator

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type RemoteNodeExecutor interface {
	WithLogger(log *zap.Logger)

	TagKeys(nodeId uint64, ShardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error)
	SeriesCardinality(nodeId uint64, database string) (int64, error)
	DeleteSeries(nodeId uint64, database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteDatabase(nodeId uint64, database string) error
	DeleteMeasurement(nodeId uint64, database, name string) error
	FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error)
	MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error)
	CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error)
	TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error)
	Stats() []StatEntity
}

type remoteNodeExecutor struct {
	ClientPool         *ClientPool
	DailTimeout        time.Duration
	ShardReaderTimeout time.Duration
	ClusterTracing     bool
	Logger             *zap.Logger
}

func (executor *remoteNodeExecutor) WithLogger(log *zap.Logger) {
	executor.Logger = log.With(zap.String("service", "RemoteNodeExecutor"))
}

func writeTestPacket(conn net.Conn) (err error) {
	conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	defer conn.SetDeadline(time.Time{})
	typ := testRequestMessage
	size := uint64(0)
	if err = binary.Write(conn, binary.BigEndian, &typ); err != nil {
		return
	}
	if err = binary.Write(conn, binary.BigEndian, &size); err != nil {
		return
	}
	return
}

func getConnWithRetry(pool *ClientPool, nodeId uint64, logger *zap.Logger) (x.PooledConn, error) {
	var (
		conn x.PooledConn
		err  error
	)
	retries := 3
	for retries > 0 {
		retries--
		conn, err = pool.GetConn(nodeId)
		if err != nil {
			logger.Warn("Failed to get connection from pool", zap.Error(err))
			return nil, ErrRetry
		}
		// do write test
		if err = writeTestPacket(conn); err != nil {
			conn.MarkUnusable()
			conn.Close()
			logger.Warn("Failed to get connection from pool", zap.Error(err))
			continue
		}
		return conn, nil
	}
	return nil, err
}

func (executor *remoteNodeExecutor) CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, err
	}
	//no need here defer conn.Close()

	var resp CreateIteratorResponse
	if err := func() error {
		var spanCtx *tracing.SpanContext
		span := tracing.SpanFromContext(ctx)
		if span != nil {
			spanCtx = new(tracing.SpanContext)
			*spanCtx = span.Context()
		}
		if err := EncodeTLV(conn, createIteratorRequestMessage, &CreateIteratorRequest{
			ShardIDs:    shardIds,
			Measurement: *m, //TODO:改为Sources
			Opt:         opt,
			SpanContex:  spanCtx,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		// Read the response.
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != nil {
			return resp.Err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	stats := query.IteratorStats{SeriesN: resp.SeriesN}
	//conn.Close will be invoked when iterator.Close
	itr := query.NewReaderIterator(ctx, newIteratorReader(conn, resp.Termination), resp.DataType, stats)
	return itr, nil
}

func (executor *remoteNodeExecutor) MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return influxql.Unknown, err
	}
	defer conn.Close()

	measurement := *m
	if measurement.SystemIterator != "" {
		measurement.Name = measurement.SystemIterator
	}

	var resp MapTypeResponse
	if err := func() error {
		if err := EncodeTLV(conn, mapTypeRequestMessage, &MapTypeRequest{
			Sources:  influxql.Sources([]influxql.Source{&measurement}),
			Field:    field,
			ShardIDs: shardIds,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return influxql.Unknown, err
	}

	return resp.DataType, nil
}

func (executor *remoteNodeExecutor) IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return query.IteratorCost{}, err
	}
	defer conn.Close()

	var resp IteratorCostResponse
	if err := func() error {
		if err := EncodeTLV(conn, iteratorCostRequestMessage, &IteratorCostRequest{
			Sources:  influxql.Sources([]influxql.Source{m}),
			Opt:      opt,
			ShardIDs: shardIds,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return query.IteratorCost{}, err
	}

	return resp.Cost, nil
}

func (executor *remoteNodeExecutor) FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	var resp FieldDimensionsResponse
	if err := func() error {
		var req FieldDimensionsRequest
		req.ShardIDs = shardIds
		req.Sources = influxql.Sources([]influxql.Source{m})
		if err := EncodeTLV(conn, fieldDimensionsRequestMessage, &req); err != nil {
			conn.MarkUnusable()
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != nil {
			return resp.Err
		}

		return nil
	}(); err != nil {
		return nil, nil, err
	}

	return resp.Fields, resp.Dimensions, nil
}

func (executor *remoteNodeExecutor) TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var resp TaskManagerStatementResponse
	if err := func() error {
		var req TaskManagerStatementRequest
		req.SetStatement(stmt.String())
		req.SetDatabase("")
		if err := EncodeTLV(conn, executeTaskManagerRequestMessage, &req); err != nil {
			conn.MarkUnusable()
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return nil, err
	}

	result := &query.Result{}
	*result = resp.Result
	return result, nil
}

func (executor *remoteNodeExecutor) SeriesCardinality(nodeId uint64, database string) (int64, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	var n int64
	if err := func() error {
		if err := EncodeTLV(conn, seriesCardinalityRequestMessage, &SeriesCardinalityRequest{
			Database: database,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp SeriesCardinalityResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		n = resp.N
		return nil
	}(); err != nil {
		return -1, err
	}

	return n, nil
}

func (executor *remoteNodeExecutor) MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var names [][]byte
	if err := func() error {
		if err := EncodeTLV(conn, measurementNamesRequestMessage, &MeasurementNamesRequest{
			Database: database,
			Cond:     cond,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp MeasurementNamesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		names = resp.Names
		return nil
	}(); err != nil {
		return nil, err
	}

	return names, nil
}

func (executor *remoteNodeExecutor) TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var tagValues []tsdb.TagValues
	if err := func() error {
		if err := EncodeTLV(conn, tagValuesRequestMessage, &TagValuesRequest{
			TagKeysRequest{
				ShardIDs: shardIDs,
				Cond:     cond,
			},
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp TagValuesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		tagValues = resp.TagValues
		return nil
	}(); err != nil {
		return nil, err
	}

	return tagValues, nil
}

func (executor *remoteNodeExecutor) TagKeys(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var tagKeys []tsdb.TagKeys
	if err := func() error {
		if err := EncodeTLV(conn, tagKeysRequestMessage, &TagKeysRequest{
			ShardIDs: shardIDs,
			Cond:     cond,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp TagKeysResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		tagKeys = resp.TagKeys
		return nil
	}(); err != nil {
		return nil, err
	}

	return tagKeys, nil
}

func (executor *remoteNodeExecutor) DeleteMeasurement(nodeId uint64, database, name string) error {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteMeasurementRequestMessage, &DeleteMeasurementRequest{
			Database: database,
			Name:     name,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp DeleteMeasurementResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (executor *remoteNodeExecutor) DeleteDatabase(nodeId uint64, database string) error {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteDatabaseRequestMessage, &DeleteDatabaseRequest{
			Database: database,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp DeleteDatabaseResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (executor *remoteNodeExecutor) DeleteSeries(nodeId uint64, database string, sources []influxql.Source, cond influxql.Expr) error {
	conn, err := getConnWithRetry(executor.ClientPool, nodeId, executor.Logger)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteSeriesRequestMessage, &DeleteSeriesRequest{
			Database: database,
			Sources:  influxql.Sources(sources),
			Cond:     cond,
		}); err != nil {
			conn.MarkUnusable()
			return err
		}

		var resp DeleteSeriesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			conn.MarkUnusable()
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (executor *remoteNodeExecutor) Stats() []StatEntity {
	return executor.ClientPool.Stat()
}

type iteratorReader struct {
	io.ReadCloser
	terminator []byte
	buf        []byte
	reader     io.ReadCloser
	judger     *x.CyclicBuffer
	terminated bool
	bytes      int
}

func newIteratorReader(rd io.ReadCloser, terminator []byte) *iteratorReader {
	return &iteratorReader{
		reader:     rd,
		terminator: terminator,
		buf:        make([]byte, len(terminator)),
		judger:     x.NewCyclicBuffer(len(terminator)),
	}
}

func shiftBuffer(data []byte, header []byte) {
	offset := len(header)
	if offset > len(data) {
		return
	}
	for i := len(data) - 1; i >= offset; i-- {
		data[i] = data[i-offset]
	}
	for i := 0; i < offset; i++ {
		data[i] = header[i]
	}
}

func (r *iteratorReader) Read(p []byte) (n int, err error) {
	if r.terminated {
		return 0, io.EOF
	}
	n, err = r.reader.Read(p)
	bytes := r.bytes
	r.bytes += n
	if n == 0 {
		return 0, err
	}
	dumped := r.judger.Dump(r.buf)
	r.judger.Write(p[:n])
	if r.bytes < len(r.terminator) {
		return 0, nil
	}
	shiftBuffer(p, r.buf[:x.Min(dumped, n)])
	if r.judger.Compare(r.terminator) {
		r.terminated = true
		err = io.EOF
	}
	if bytes < len(r.buf) {
		return n - (len(r.buf) - bytes), err
	}
	return n, err
}

func (r *iteratorReader) consumeRest() (discarded int) {
	if r.terminated {
		return
	}
	buf := make([]byte, 32)
	for {
		n, err := r.Read(buf)
		discarded += n
		if err == io.EOF {
			break
		}
	}
	return
}

func (r *iteratorReader) Close() error {
	r.consumeRest()
	return r.reader.Close()
}
