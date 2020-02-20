package coordinator

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type RemoteNodeExecutor interface {
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
}

type remoteNodeExecutor struct {
	MetaClient interface {
		DataNode(nodeId uint64) (*meta.NodeInfo, error)
	}
	DailTimeout        time.Duration
	ShardReaderTimeout time.Duration
	ClusterTracing     bool
}

func (me *remoteNodeExecutor) CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.ShardReaderTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		// Read the response.
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	if resp.DataType == influxql.Unknown {
		return nil, nil
	}

	stats := query.IteratorStats{SeriesN: resp.SeriesN}
	//conn.Close will be invoked when iterator.Close
	itr := query.NewReaderIterator(ctx, conn, resp.DataType, stats)
	return itr, nil
}

func (me *remoteNodeExecutor) MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) SeriesCardinality(nodeId uint64, database string) (int64, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	var n int64
	if err := func() error {
		if err := EncodeTLV(conn, seriesCardinalityRequestMessage, &SeriesCardinalityRequest{
			Database: database,
		}); err != nil {
			return err
		}

		var resp SeriesCardinalityResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		var resp MeasurementNamesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		var resp TagValuesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) TagKeys(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		var resp TagKeysResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) DeleteMeasurement(nodeId uint64, database, name string) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteMeasurementRequestMessage, &DeleteMeasurementRequest{
			Database: database,
			Name:     name,
		}); err != nil {
			return err
		}

		var resp DeleteMeasurementResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) DeleteDatabase(nodeId uint64, database string) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteDatabaseRequestMessage, &DeleteDatabaseRequest{
			Database: database,
		}); err != nil {
			return err
		}

		var resp DeleteDatabaseResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

func (me *remoteNodeExecutor) DeleteSeries(nodeId uint64, database string, sources []influxql.Source, cond influxql.Expr) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
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
			return err
		}

		var resp DeleteSeriesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
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

// NodeDialer dials connections to a given node.
type NodeDialer struct {
	MetaClient interface {
		DataNode(nodeId uint64) (*meta.NodeInfo, error)
	}
	Timeout time.Duration
}

// DialNode returns a connection to a node.
func (d *NodeDialer) DialNode(nodeID uint64) (net.Conn, error) {
	ni, err := d.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", ni.TCPHost)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(d.Timeout))

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
