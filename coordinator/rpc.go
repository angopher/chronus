package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/angopher/chronus/coordinator/internal"
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"regexp"
	"time"
)

//go:generate protoc --gogo_out=. internal/data.proto

// WritePointsRequest represents a request to write point data to the cluster
type WritePointsRequest struct {
	Database        string
	RetentionPolicy string
	//ConsistencyLevel models.ConsistencyLevel
	Points []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags models.Tags) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// WriteShardRequest represents the a request to write a slice of points to a shard
type WriteShardRequest struct {
	pb internal.WriteShardRequest
}

// WriteShardResponse represents the response returned from a remote WriteShardRequest call
type WriteShardResponse struct {
	pb internal.WriteShardResponse
}

// SetShardID sets the ShardID
func (w *WriteShardRequest) SetShardID(id uint64) { w.pb.ShardID = &id }

// ShardID gets the ShardID
func (w *WriteShardRequest) ShardID() uint64 { return w.pb.GetShardID() }

func (w *WriteShardRequest) SetDatabase(db string) { w.pb.Database = &db }

func (w *WriteShardRequest) SetRetentionPolicy(rp string) { w.pb.RetentionPolicy = &rp }

func (w *WriteShardRequest) Database() string { return w.pb.GetDatabase() }

func (w *WriteShardRequest) RetentionPolicy() string { return w.pb.GetRetentionPolicy() }

// Points returns the time series Points
func (w *WriteShardRequest) Points() []models.Point { return w.unmarshalPoints() }

// AddPoint adds a new time series point
func (w *WriteShardRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags models.Tags) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.AddPoints([]models.Point{pt})
}

// AddPoints adds a new time series point
func (w *WriteShardRequest) AddPoints(points []models.Point) {
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			// A error here means that we create a point higher in the stack that we could
			// not marshal to a byte slice.  If that happens, the endpoint that created that
			// point needs to be fixed.
			panic(fmt.Sprintf("failed to marshal point: `%v`: %v", p, err))
		}
		w.pb.Points = append(w.pb.Points, b)
	}
}

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

func (w *WriteShardRequest) unmarshalPoints() []models.Point {
	points := make([]models.Point, len(w.pb.GetPoints()))
	for i, p := range w.pb.GetPoints() {
		pt, err := models.NewPointFromBytes(p)
		if err != nil {
			// A error here means that one node created a valid point and sent us an
			// unparseable version.  We could log and drop the point and allow
			// anti-entropy to resolve the discrepancy, but this shouldn't ever happen.
			panic(fmt.Sprintf("failed to parse point: `%v`: %v", string(p), err))
		}

		points[i] = pt
	}
	return points
}

// SetCode sets the Code
func (w *WriteShardResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// SetMessage sets the Message
func (w *WriteShardResponse) SetMessage(message string) { w.pb.Message = &message }

// Code returns the Code
func (w *WriteShardResponse) Code() int { return int(w.pb.GetCode()) }

// Message returns the Message
func (w *WriteShardResponse) Message() string { return w.pb.GetMessage() }

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementRequest represents the a request to execute a statement on a node.
type ExecuteStatementRequest struct {
	pb internal.ExecuteStatementRequest
}

// Statement returns the InfluxQL statement.
func (r *ExecuteStatementRequest) Statement() string { return r.pb.GetStatement() }

// SetStatement sets the InfluxQL statement.
func (r *ExecuteStatementRequest) SetStatement(statement string) {
	r.pb.Statement = proto.String(statement)
}

// Database returns the database name.
func (r *ExecuteStatementRequest) Database() string { return r.pb.GetDatabase() }

// SetDatabase sets the database name.
func (r *ExecuteStatementRequest) SetDatabase(database string) { r.pb.Database = proto.String(database) }

// MarshalBinary encodes the object to a binary format.
func (r *ExecuteStatementRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.pb)
}

// UnmarshalBinary populates ExecuteStatementRequest from a binary format.
func (r *ExecuteStatementRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &r.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementResponse represents the response returned from a remote ExecuteStatementRequest call.
type ExecuteStatementResponse struct {
	pb internal.WriteShardResponse
}

// Code returns the response code.
func (w *ExecuteStatementResponse) Code() int { return int(w.pb.GetCode()) }

// SetCode sets the Code
func (w *ExecuteStatementResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// Message returns the repsonse message.
func (w *ExecuteStatementResponse) Message() string { return w.pb.GetMessage() }

// SetMessage sets the Message
func (w *ExecuteStatementResponse) SetMessage(message string) { w.pb.Message = &message }

// MarshalBinary encodes the object to a binary format.
func (w *ExecuteStatementResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates ExecuteStatementResponse from a binary format.
func (w *ExecuteStatementResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

type TaskManagerStatementRequest struct {
	ExecuteStatementRequest
}

type TaskManagerStatementResponse struct {
	Result query.Result
	Err    string
}

type TaskManagerStatementRespProto struct {
	Result []byte
	Err    string
}

func (w *TaskManagerStatementResponse) MarshalBinary() ([]byte, error) {
	var proto TaskManagerStatementRespProto
	buf, err := w.Result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	proto.Result = buf
	proto.Err = w.Err
	return json.Marshal(proto)
}

func (w *TaskManagerStatementResponse) UnmarshalBinary(buf []byte) error {
	var proto TaskManagerStatementRespProto
	err := json.Unmarshal(buf, &proto)
	if err != nil {
		return nil
	}
	w.Err = proto.Err
	return w.Result.UnmarshalJSON(proto.Result)
}

type MapTypeRequest struct {
	Sources  influxql.Sources
	Field    string
	ShardIDs []uint64
}
type MapTypeRequestProto struct {
	Sources  []byte
	Field    string
	ShardIDs []uint64
}

func (r *MapTypeRequest) MarshalBinary() ([]byte, error) {
	var proto MapTypeRequestProto
	proto.ShardIDs = r.ShardIDs
	proto.Field = r.Field
	var err error
	if proto.Sources, err = r.Sources.MarshalBinary(); err != nil {
		return nil, err
	}
	return json.Marshal(proto)
}

func (r *MapTypeRequest) UnmarshalBinary(b []byte) error {
	var proto MapTypeRequestProto
	if err := json.Unmarshal(b, &proto); err != nil {
		return err
	}
	if err := r.Sources.UnmarshalBinary(proto.Sources); err != nil {
		return err
	}
	r.Field = proto.Field
	r.ShardIDs = proto.ShardIDs
	return nil
}

type MapTypeResponse struct {
	DataType influxql.DataType
	Err      string
}

func (r *MapTypeResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

func (r *MapTypeResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type IteratorCostRequest struct {
	Sources  influxql.Sources
	Opt      query.IteratorOptions
	ShardIDs []uint64
}
type IteartorCostRequestProto struct {
	Sources  []byte
	Opt      []byte
	ShardIDs []uint64
}

func (r *IteratorCostRequest) MarshalBinary() ([]byte, error) {
	var proto IteartorCostRequestProto
	proto.ShardIDs = r.ShardIDs
	var err error
	if proto.Sources, err = r.Sources.MarshalBinary(); err != nil {
		return nil, err
	}
	if proto.Opt, err = r.Opt.MarshalBinary(); err != nil {
		return nil, err
	}

	return json.Marshal(proto)
}

func (r *IteratorCostRequest) UnmarshalBinary(b []byte) error {
	var proto IteartorCostRequestProto
	if err := json.Unmarshal(b, &proto); err != nil {
		return err
	}
	if err := r.Sources.UnmarshalBinary(proto.Sources); err != nil {
		return err
	}
	if err := r.Opt.UnmarshalBinary(proto.Opt); err != nil {
		return err
	}
	r.ShardIDs = proto.ShardIDs
	return nil
}

type IteratorCostResponse struct {
	Cost query.IteratorCost
	Err  string
}

func (r *IteratorCostResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

func (r *IteratorCostResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type DeleteMeasurementRequest struct {
	Database string
	Name     string
}

func (r *DeleteMeasurementRequest) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *DeleteMeasurementRequest) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type DeleteMeasurementResponse struct {
	Err string
}

func (r *DeleteMeasurementResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *DeleteMeasurementResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type DeleteDatabaseRequest struct {
	Database string
}

func (r *DeleteDatabaseRequest) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *DeleteDatabaseRequest) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type DeleteDatabaseResponse struct {
	Err string
}

func (r *DeleteDatabaseResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *DeleteDatabaseResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type DeleteSeriesRequest struct {
	Database string
	Sources  influxql.Sources
	Cond     influxql.Expr
}

type DeleteSeriesRequestProto struct {
	Database string
	Sources  []byte
	Cond     string
}

func (r *DeleteSeriesRequest) MarshalBinary() ([]byte, error) {
	var proto DeleteSeriesRequestProto
	proto.Database = r.Database
	var err error
	if proto.Sources, err = r.Sources.MarshalBinary(); err != nil {
		return nil, err
	}
	if r.Cond != nil {
		proto.Cond = r.Cond.String()
	}
	return json.Marshal(proto)
}

func (r *DeleteSeriesRequest) UnmarshalBinary(b []byte) error {
	var proto DeleteSeriesRequestProto
	err := json.Unmarshal(b, &proto)
	if err != nil {
		return err
	}

	if err = (&r.Sources).UnmarshalBinary(proto.Sources); err != nil {
		return err
	}
	r.Database = proto.Database
	if proto.Cond != "" {
		r.Cond, err = influxql.ParseExpr(proto.Cond)
	}
	return err
}

type DeleteSeriesResponse struct {
	Err string
}

func (r *DeleteSeriesResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *DeleteSeriesResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type SeriesCardinalityRequest struct {
	Database string
}

func (r *SeriesCardinalityRequest) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *SeriesCardinalityRequest) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type SeriesCardinalityResponse struct {
	N   int64
	Err string
}

func (r *SeriesCardinalityResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}
func (r *SeriesCardinalityResponse) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, r)
}

type MeasurementNamesRequest struct {
	Database string
	Cond     influxql.Expr
}

type MeasurementNamesProto struct {
	Database string
	Cond     string
}

func (r *MeasurementNamesRequest) MarshalBinary() ([]byte, error) {
	var proto MeasurementNamesProto
	if r.Cond != nil {
		proto.Cond = r.Cond.String()
	}
	proto.Database = r.Database

	return json.Marshal(proto)
}

func (r *MeasurementNamesRequest) UnmarshalBinary(b []byte) error {
	var proto MeasurementNamesProto
	err := json.Unmarshal(b, &proto)
	if err != nil {
		return err
	}

	r.Database = proto.Database
	if proto.Cond != "" {
		r.Cond, err = influxql.ParseExpr(proto.Cond)
	}
	return err
}

type MeasurementNamesResponse struct {
	Names [][]byte
	Err   string
}

func (r *MeasurementNamesResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

func (r *MeasurementNamesResponse) UnmarshalBinary(b []byte) error {
	err := json.Unmarshal(b, r)
	return err
}

type TagKeysRequest struct {
	ShardIDs []uint64
	Cond     influxql.Expr
}

type TagKeysProto struct {
	ShardIDs []byte
	Cond     string
}

func (r *TagKeysRequest) MarshalBinary() ([]byte, error) {
	var proto TagKeysProto
	if r.Cond != nil {
		proto.Cond = r.Cond.String()
	}
	var err error
	proto.ShardIDs, err = json.Marshal(r.ShardIDs)
	if err != nil {
		return nil, err
	}

	return json.Marshal(proto)
}

func (r *TagKeysRequest) UnmarshalBinary(b []byte) error {
	var proto TagKeysProto
	err := json.Unmarshal(b, &proto)
	if err != nil {
		return err
	}

	if proto.Cond != "" {
		r.Cond, err = influxql.ParseExpr(proto.Cond)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(proto.ShardIDs, &r.ShardIDs)
}

type TagKeysResponse struct {
	TagKeys []tsdb.TagKeys
	Err     string
}

func (r *TagKeysResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

func (r *TagKeysResponse) UnmarshalBinary(b []byte) error {
	err := json.Unmarshal(b, r)
	return err
}

type TagValuesRequest struct {
	TagKeysRequest
}

type TagValuesResponse struct {
	TagValues []tsdb.TagValues
	Err       string
}

func (r *TagValuesResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

func (r *TagValuesResponse) UnmarshalBinary(b []byte) error {
	err := json.Unmarshal(b, r)
	return err
}

// CreateIteratorRequest represents a request to create a remote iterator.
type CreateIteratorRequest struct {
	SpanContex  *tracing.SpanContext
	ShardIDs    []uint64
	Opt         query.IteratorOptions
	Measurement influxql.Measurement
}

type Measurement struct {
	Database        string
	RetentionPolicy string
	Name            string
	Regex           string
	IsTarget        bool
	// This field indicates that the measurement should read be read from the
	// specified system iterator.
	SystemIterator string
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}

	mm := Measurement{
		Database:        r.Measurement.Database,
		RetentionPolicy: r.Measurement.RetentionPolicy,
		Name:            r.Measurement.Name,
		IsTarget:        r.Measurement.IsTarget,
		SystemIterator:  r.Measurement.SystemIterator,
	}

	if r.Measurement.Regex != nil {
		mm.Regex = r.Measurement.Regex.Val.String()
	}

	mbuf, err := json.Marshal(mm)
	if err != nil {
		return nil, err
	}

	var sbuf []byte
	if r.SpanContex != nil {
		if sbuf, err = r.SpanContex.MarshalBinary(); err != nil {
			return nil, err
		}
	}

	return proto.Marshal(&internal.CreateIteratorRequest{
		ShardIDs:    r.ShardIDs,
		Measurement: mbuf,
		Opt:         buf,
		SpanContext: sbuf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}

	var mm Measurement
	if err := json.Unmarshal(pb.GetMeasurement(), &mm); err != nil {
		return err
	}

	r.Measurement = influxql.Measurement{
		Database:        mm.Database,
		RetentionPolicy: mm.RetentionPolicy,
		Name:            mm.Name,
		IsTarget:        mm.IsTarget,
		SystemIterator:  mm.SystemIterator,
	}

	if len(mm.Regex) > 0 {
		regex, err := regexp.Compile(mm.Regex)
		if err != nil {
			return err
		}
		r.Measurement.Regex = &influxql.RegexLiteral{Val: regex}
	}

	sbuf := pb.GetSpanContext()
	if sbuf != nil {
		r.SpanContex = new(tracing.SpanContext)
		if err := r.SpanContex.UnmarshalBinary(sbuf); err != nil {
			return err
		}
	}

	return nil
}

// CreateIteratorResponse represents a response from remote iterator creation.
type CreateIteratorResponse struct {
	DataType influxql.DataType
	SeriesN  int
	Err      error
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorResponse) MarshalBinary() ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorResponse) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, r)
}

// FieldDimensionsRequest represents a request to retrieve unique fields & dimensions.
type FieldDimensionsRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.FieldDimensionsRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsRequest) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// FieldDimensionsResponse represents a response from remote iterator creation.
type FieldDimensionsResponse struct {
	Fields     map[string]influxql.DataType
	Dimensions map[string]struct{}
	Err        error
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsResponse) MarshalBinary() ([]byte, error) {
	var pb internal.FieldDimensionsResponse

	buf, err := json.Marshal(r.Fields)
	if err != nil {
		return nil, err
	}
	pb.Fields = buf
	pb.Dimensions = make([]string, 0, len(r.Dimensions))
	for k := range r.Dimensions {
		pb.Dimensions = append(pb.Dimensions, k)
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsResponse) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if err := json.Unmarshal(pb.Fields, &r.Fields); err != nil {
		return err
	}

	r.Dimensions = make(map[string]struct{}, len(pb.GetDimensions()))
	for _, s := range pb.GetDimensions() {
		r.Dimensions[s] = struct{}{}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}
