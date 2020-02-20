package coordinator_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func TestClusterExecuterTagKeys(t *testing.T) {
	//TODO
}

func TestClusterExecuterTagValues(t *testing.T) {
	//TODO
}

func TestClusterExecuterMeasurementNames(t *testing.T) {
	//TODO
}

func TestClusterExecuterSeriesCardinality(t *testing.T) {
	//TODO
}

func TestClusterExecuterFieldDimensions(t *testing.T) {
	//TODO
}

func TestClusterExecuterDeleteSeries(t *testing.T) {
	//TODO
}

func TestClusterExecuterDeleteDatabase(t *testing.T) {
	//TODO
}

func TestClusterExecuterDeleteMeasurement(t *testing.T) {
	//TODO
}

func TestClusterExecuterIteratorCost(t *testing.T) {
	//TODO
}

func TestClusterExecuterMapType(t *testing.T) {
	//TODO
}

func TestClusterExecuterCreateIterator(t *testing.T) {
	//TODO
}

func TestClusterExecuterTaskManagerStatement(t *testing.T) {
	//TODO
}

type fakeMetaClient struct {
	DataNodesFn  func() ([]meta.NodeInfo, error)
	DataNodeFn   func(nodeId uint64) (*meta.NodeInfo, error)
	ShardOwnerFn func(id uint64) (string, string, *meta.ShardGroupInfo)
	DatabaseFn   func(name string) *meta.DatabaseInfo
}

func (f *fakeMetaClient) DataNodes() ([]meta.NodeInfo, error) {
	return f.DataNodesFn()
}

func (f *fakeMetaClient) DataNode(nodeId uint64) (*meta.NodeInfo, error) {
	return f.DataNodeFn(nodeId)
}

func (f *fakeMetaClient) ShardOwner(id uint64) (string, string, *meta.ShardGroupInfo) {
	return f.ShardOwnerFn(id)
}

func (f *fakeMetaClient) Database(name string) *meta.DatabaseInfo {
	return f.DatabaseFn(name)
}

type fakeTSDBStore struct {
	DeleteShardFn       func(id uint64) error
	DeleteDatabaseFn    func(name string) error
	DeleteMeasurementFn func(database, name string) error
	TagKeysFn           func(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValuesFn         func(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	ShardGroupFn        func(ids []uint64) tsdb.ShardGroup
	MeasurementNamesFn  func(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	SeriesCardinalityFn func(database string) (int64, error)
}

func (f *fakeTSDBStore) DeleteShard(id uint64) error {
	return f.DeleteShardFn(id)
}

func (f *fakeTSDBStore) DeleteDatabase(name string) error {
	return f.DeleteDatabaseFn(name)
}

func (f *fakeTSDBStore) DeleteMeasurement(database, name string) error {
	return f.DeleteMeasurementFn(database, name)
}

func (f *fakeTSDBStore) TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	return f.TagKeysFn(auth, shardIDs, cond)
}

func (f *fakeTSDBStore) TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	return f.TagValuesFn(auth, shardIDs, cond)
}

func (f *fakeTSDBStore) ShardGroup(ids []uint64) tsdb.ShardGroup {
	return f.ShardGroupFn(ids)
}

func (f *fakeTSDBStore) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	return f.MeasurementNamesFn(auth, database, cond)
}

func (f *fakeTSDBStore) SeriesCardinality(database string) (int64, error) {
	return f.SeriesCardinalityFn(database)
}

type fakeTaskManager struct {
	ExecuteFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (f *fakeTaskManager) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	return f.ExecuteFn(stmt, ctx)
}

type fakeRemoteNode struct {
	TagKeysFn              func(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValuesFn            func(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	MeasurementNamesFn     func(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error)
	SeriesCardinalityFn    func(nodeId uint64, database string) (int64, error)
	DeleteSeriesFn         func(nodeId uint64, database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteDatabaseFn       func(nodeId uint64, database string) error
	DeleteMeasurementFn    func(nodeId uint64, database, name string) error
	FieldDimensionsFn      func(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	IteratorCostFn         func(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error)
	MapTypeFn              func(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error)
	CreateIteratorFn       func(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error)
	TaskManagerStatementFn func(nodeId uint64, stmt influxql.Statement) (*query.Result, error)
}

func (f *fakeRemoteNode) TagKeys(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	return f.TagKeysFn(nodeId, shardIDs, cond)
}

func (f *fakeRemoteNode) TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	return f.TagValuesFn(nodeId, shardIDs, cond)
}

func (f *fakeRemoteNode) MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error) {
	return f.MeasurementNamesFn(nodeId, database, cond)
}

func (f *fakeRemoteNode) SeriesCardinality(nodeId uint64, database string) (int64, error) {
	return f.SeriesCardinalityFn(nodeId, database)
}

func (f *fakeRemoteNode) DeleteSeries(nodeId uint64, database string, sources []influxql.Source, condition influxql.Expr) error {
	return f.DeleteSeriesFn(nodeId, database, sources, condition)
}

func (f *fakeRemoteNode) DeleteDatabase(nodeId uint64, database string) error {
	return f.DeleteDatabaseFn(nodeId, database)
}

func (f *fakeRemoteNode) DeleteMeasurement(nodeId uint64, database, name string) error {
	return f.DeleteMeasurementFn(nodeId, database, name)
}

func (f *fakeRemoteNode) FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return f.FieldDimensionsFn(nodeId, m, shardIds)
}

func (f *fakeRemoteNode) IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error) {
	return f.IteratorCostFn(nodeId, m, opt, shardIds)
}

func (f *fakeRemoteNode) MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error) {
	return f.MapTypeFn(nodeId, m, field, shardIds)
}

func (f *fakeRemoteNode) CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error) {
	return f.CreateIteratorFn(nodeId, ctx, m, opt, shardIds)
}

func (f *fakeRemoteNode) TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error) {
	return f.TaskManagerStatementFn(nodeId, stmt)
}
