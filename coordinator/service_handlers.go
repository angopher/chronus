package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/angopher/chronus/x"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

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
			s.Logger.Error("drop write request: no database or retention policy received\n",
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

func (s *Service) processCreateIteratorRequest(conn io.ReadWriter, buf []byte) (ioError bool) {
	var itr query.Iterator
	var trace *tracing.Trace
	var span *tracing.Span
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
		defer itr.Close()
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
	defer func() {
		// Write termination of iterator
		if _, err := conn.Write(itrTerminator); err != nil {
			ioError = true
		}
	}()

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

	if trace != nil {
		span.Finish()
		if err := encoder.EncodeTrace(trace); err != nil {
			s.Logger.Error("EncodeTrace fail", zap.Error(err))
			atomic.AddInt64(&s.stats.CreateIteratorFail, 1)
			ioError = true
			return
		}
	}
	return
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
