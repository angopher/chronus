package coordinator

import (
	"context"
	"time"

	"fmt"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

// ClusterShardMapper implements a ShardMapper for local shards.
type ClusterShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}
	Node            *influxdb.Node
	ClusterExecutor interface {
		IteratorCost(m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.IteratorCost, error)
		MapType(m *influxql.Measurement, field string, shards []meta.ShardInfo) influxql.DataType
		CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.Iterator, error)
		FieldDimensions(m *influxql.Measurement, shards []meta.ShardInfo) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *ClusterShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &ClusterShardMapping{
		MetaClient:      e.MetaClient,
		Node:            e.Node,
		shardInfos:      make(map[Source][]meta.ShardInfo),
		ClusterExecutor: e.ClusterExecutor,
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *ClusterShardMapper) mapShards(a *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.shardInfos[source]; !ok {
				//groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.shardInfos[source] = nil
					continue
				}

				num := 0
				for _, g := range groups {
					num += len(g.Shards)
				}

				shards := make([]meta.ShardInfo, 0, num)
				for _, g := range groups {
					for _, si := range g.Shards {
						shards = append(shards, si)
					}
				}
				a.shardInfos[source] = shards
			}
		case *influxql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

// ShardMapper maps data sources to a list of shard information.
type ClusterShardMapping struct {
	shardInfos      map[Source][]meta.ShardInfo
	ClusterExecutor interface {
		IteratorCost(m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.IteratorCost, error)
		MapType(m *influxql.Measurement, field string, shards []meta.ShardInfo) influxql.DataType
		CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.Iterator, error)
		FieldDimensions(m *influxql.Measurement, shards []meta.ShardInfo) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	}
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}
	Node *influxdb.Node

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time
}

func (a *ClusterShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	shards := a.shardInfos[source]
	if shards == nil {
		return
	}

	f, d, err := a.ClusterExecutor.FieldDimensions(m, shards)
	if err != nil {
		return nil, nil, err
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}
	return
}

func (a *ClusterShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	shards := a.shardInfos[source]
	if shards == nil {
		return influxql.Unknown
	}

	return a.ClusterExecutor.MapType(m, field, shards)
}

func (a *ClusterShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	shards := a.shardInfos[source]
	if shards == nil {
		return nil, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	return a.ClusterExecutor.CreateIterator(ctx, m, opt, shards)
}

func (a *ClusterShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	shards := a.shardInfos[source]
	if shards == nil {
		return query.IteratorCost{}, fmt.Errorf("not find source %+v", source)
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	return a.ClusterExecutor.IteratorCost(m, opt, shards)
}

// Close clears out the list of mapped shards.
func (a *ClusterShardMapping) Close() error {
	a.shardInfos = nil
	return nil
}

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}
