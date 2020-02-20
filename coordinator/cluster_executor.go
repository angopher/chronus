package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type ClusterExecutor struct {
	TSDBStore
	Node       *influxdb.Node
	MetaClient interface {
		DataNodes() ([]meta.NodeInfo, error)
		DataNode(nodeId uint64) (*meta.NodeInfo, error)
		ShardOwner(id uint64) (string, string, *meta.ShardGroupInfo)
		Database(name string) *meta.DatabaseInfo
	}

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager query.StatementExecutor

	RemoteNodeExecutor RemoteNodeExecutor
	Logger             *zap.Logger
}

func NewClusterExecutor(n *influxdb.Node, s TSDBStore, m MetaClient, Config Config) *ClusterExecutor {
	return &ClusterExecutor{
		Node:       n,
		TSDBStore:  s,
		MetaClient: m,
		RemoteNodeExecutor: &remoteNodeExecutor{
			MetaClient:         m,
			DailTimeout:        time.Duration(Config.DailTimeout),
			ShardReaderTimeout: time.Duration(Config.ShardReaderTimeout),
			ClusterTracing:     Config.ClusterTracing,
		},
		Logger: zap.NewNop(),
	}
}

func (me *ClusterExecutor) WithLogger(log *zap.Logger) {
	me.Logger = log.With(zap.String("service", "ClusterExecutor"))
}

func (me *ClusterExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext) error {
	type Result struct {
		qr  *query.Result
		err error
	}

	nodeInfos, err := me.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	nodes := NewNodeIdsByNodes(nodeInfos)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		host := "unkown"
		node, _ := me.MetaClient.DataNode(nodeId)
		if node != nil {
			host = node.Host
		}

		switch t := stmt.(type) {
		case *influxql.KillQueryStatement:
			if t.Host != "" && t.Host != host {
				return
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			var qr *query.Result
			if nodeId == me.Node.ID {
				recvCtx := &query.ExecutionContext{
					Context: context.Background(),
					Results: make(chan *query.Result, 1),
				}
				err = me.TaskManager.ExecuteStatement(stmt, recvCtx)
				if err == nil {
					qr = <-recvCtx.Results
				}
			} else {
				qr, err = me.RemoteNodeExecutor.TaskManagerStatement(nodeId, stmt)
			}

			if qr != nil && len(qr.Series) > 0 {
				qr.Series[0].Columns = append(qr.Series[0].Columns, "host")
				for i := 0; i < len(qr.Series[0].Values); i++ {
					qr.Series[0].Values[i] = append(qr.Series[0].Values[i], host)
				}
			}

			mutex.Lock()
			results[nodeId] = &Result{qr: qr, err: err}
			mutex.Unlock()
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	//merge result
	row := new(models.Row)
	err = nil
	switch stmt.(type) {
	case *influxql.ShowQueriesStatement:
		for _, r := range results {
			if r.err != nil {
				err = r.err
				break
			}
			if len(r.qr.Series) == 0 {
				continue
			}
			if len(row.Columns) == 0 {
				row.Columns = r.qr.Series[0].Columns
			}
			row.Values = append(row.Values, r.qr.Series[0].Values...)
		}
	case *influxql.KillQueryStatement:
		allFail := true
		for _, r := range results {
			if r.err != nil {
				err = r.err
			} else {
				allFail = false
			}
		}
		if !allFail {
			err = nil
		}
	}

	if err != nil {
		return err
	}
	ctx.Send(&query.Result{Series: models.Rows{row}})
	return nil
}

func (me *ClusterExecutor) SeriesCardinality(database string) (int64, error) {
	type Result struct {
		n   int64
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return -1, err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var n int64
			var err error
			if nodeId == me.Node.ID {
				n, err = me.TSDBStore.SeriesCardinality(database)
			} else {
				n, err = me.RemoteNodeExecutor.SeriesCardinality(nodeId, database)
			}

			mutex.Lock()
			results[nodeId] = &Result{n: n, err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	var sum int64
	for _, r := range results {
		if r.err != nil {
			return -1, r.err
		}
		sum += r.n
	}
	return sum, nil
}

func DatabaseShards(c interface {
	Database(name string) *meta.DatabaseInfo
}, db string) ([]meta.ShardInfo, error) {
	dbInfo := c.Database(db)
	if dbInfo == nil {
		return nil, fmt.Errorf("not find database %s", db)
	}
	var shards []meta.ShardInfo
	for _, rp := range dbInfo.RetentionPolicies {
		for _, sg := range rp.ShardGroups {
			shards = append(shards, sg.Shards...)
		}
	}

	return shards, nil
}

func (me *ClusterExecutor) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	type Result struct {
		names [][]byte
		err   error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return nil, err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var names [][]byte
			var err error
			if nodeId == me.Node.ID {
				names, err = me.TSDBStore.MeasurementNames(auth, database, cond)
			} else {
				names, err = me.RemoteNodeExecutor.MeasurementNames(nodeId, database, cond)
			}

			mutex.Lock()
			results[nodeId] = &Result{names: names, err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	uniq := make(map[string]struct{})
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for _, name := range r.names {
			uniq[string(name)] = struct{}{}
		}
	}

	strNames := make([]string, 0, len(uniq))
	for name, _ := range uniq {
		strNames = append(strNames, name)
	}
	sort.Sort(StringSlice(strNames))

	names := make([][]byte, 0, len(uniq))
	for _, name := range strNames {
		names = append(names, []byte(name))
	}

	return names, nil
}

func (me *ClusterExecutor) TagValues(auth query.Authorizer, ids []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	type TagValuesResult struct {
		values []tsdb.TagValues
		err    error
	}

	shards, err := GetShardInfoByIds(me.MetaClient, ids)
	if err != nil {
		return nil, err
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	result := make(map[uint64]*TagValuesResult)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var tagValues []tsdb.TagValues
			var err error
			if nodeId == me.Node.ID {
				tagValues, err = me.TSDBStore.TagValues(auth, shardIDs, cond)
			} else {
				tagValues, err = me.RemoteNodeExecutor.TagValues(nodeId, shardIDs, cond)
			}

			mutex.Lock()
			result[nodeId] = &TagValuesResult{values: tagValues, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	extractKeyFn := func(kv tsdb.KeyValue) string {
		//TODO:
		return fmt.Sprintf("key=%s+-*/value=%s", kv.Key, kv.Value)
	}

	uniq := make(map[string]map[string]tsdb.KeyValue)
	for nodeId, r := range result {
		if r.err != nil {
			return nil, fmt.Errorf("TagValues fail, nodeId %d, err:%s", nodeId, r.err)
		}

		for _, tagValue := range r.values {
			m, ok := uniq[tagValue.Measurement]
			if !ok {
				m = make(map[string]tsdb.KeyValue)
			}
			for _, kv := range tagValue.Values {
				m[extractKeyFn(kv)] = kv
			}
			uniq[tagValue.Measurement] = m
		}
	}

	tagValues := make([]tsdb.TagValues, len(uniq))
	idx := 0
	for m, kvs := range uniq {
		tagv := &tagValues[idx]
		tagv.Measurement = m
		for _, kv := range kvs {
			tagv.Values = append(tagv.Values, kv)
		}
		sort.Sort(tsdb.KeyValues(tagv.Values))
		idx = idx + 1
	}

	sort.Sort(tsdb.TagValuesSlice(tagValues))
	return tagValues, nil
}

func (me *ClusterExecutor) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.Iterator, error) {
	type Result struct {
		iter query.Iterator
		err  error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var iter query.Iterator
			var err error
			if nodeId == me.Node.ID {
				//localCtx only use for local node
				localCtx := ctx
				iter, err = func() (query.Iterator, error) {
					span := tracing.SpanFromContext(localCtx)
					if span != nil {
						span = span.StartSpan(fmt.Sprintf("local_node_id: %d", me.Node.ID))
						defer span.Finish()

						localCtx = tracing.NewContextWithSpan(localCtx, span)
					}
					sg := me.TSDBStore.ShardGroup(shardIDs)
					if m.Regex != nil {
						measurements := sg.MeasurementsByRegex(m.Regex.Val)
						inputs := make([]query.Iterator, 0, len(measurements))
						if err := func() error {
							for _, measurement := range measurements {
								mm := m.Clone()
								mm.Name = measurement
								input, err := sg.CreateIterator(localCtx, mm, opt)
								if err != nil {
									return err
								}
								inputs = append(inputs, input)
							}
							return nil
						}(); err != nil {
							query.Iterators(inputs).Close()
							return nil, err
						}

						return query.Iterators(inputs).Merge(opt)
					}
					return sg.CreateIterator(localCtx, m, opt)
				}()
			} else {
				iter, err = me.RemoteNodeExecutor.CreateIterator(nodeId, ctx, m, opt, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{iter: iter, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	seriesN := 0
	inputs := make([]query.Iterator, 0, len(results))
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if r.iter != nil {
			stats := r.iter.Stats()
			seriesN += stats.SeriesN
			inputs = append(inputs, r.iter)
		}
	}

	if opt.MaxSeriesN > 0 && seriesN > opt.MaxSeriesN {
		query.Iterators(inputs).Close()
		return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", seriesN, opt.MaxSeriesN)
	}
	return query.Iterators(inputs).Merge(opt)
}

func (me *ClusterExecutor) MapType(m *influxql.Measurement, field string, shards []meta.ShardInfo) influxql.DataType {
	type Result struct {
		dataType influxql.DataType
		err      error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	var result Result
	fn := func(nodeId uint64, shardIDs []uint64) {
		if nodeId == me.Node.ID {
			sg := me.TSDBStore.ShardGroup(shardIDs)
			var names []string
			if m.Regex != nil {
				names = sg.MeasurementsByRegex(m.Regex.Val)
			} else {
				names = []string{m.Name}
			}

			typ := influxql.Unknown
			for _, name := range names {
				if m.SystemIterator != "" {
					name = m.SystemIterator
				}
				t := sg.MapType(name, field)
				if typ.LessThan(t) {
					typ = t
				}
			}
			result.dataType = typ
		}
		return
	}

	n2s.Apply(fn)
	if result.dataType != influxql.Unknown {
		return result.dataType
	}

	//本地失败, 尝试从remote node获取
	results := make(map[uint64]*Result)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn = func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			if nodeId != me.Node.ID {
				mutex.Lock()
				typ, err := me.RemoteNodeExecutor.MapType(nodeId, m, field, shardIDs)
				results[nodeId] = &Result{dataType: typ, err: err}
				mutex.Unlock()
			}
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	typ := influxql.Unknown
	for nodeId, r := range results {
		if r.err != nil {
			me.Logger.Warn("results have error", zap.Error(r.err), zap.Uint64("node", nodeId))
			continue
		}
		if typ.LessThan(r.dataType) {
			typ = r.dataType
		}
	}

	return typ
}

func (me *ClusterExecutor) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.IteratorCost, error) {
	type Result struct {
		cost query.IteratorCost
		err  error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var cost query.IteratorCost
			var err error
			if nodeId == me.Node.ID {
				sg := me.TSDBStore.ShardGroup(shardIDs)
				if m.Regex != nil {
					cost, err = func() (query.IteratorCost, error) {
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
					cost, err = sg.IteratorCost(m.Name, opt)
				}
			} else {
				cost, err = me.RemoteNodeExecutor.IteratorCost(nodeId, m, opt, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{cost: cost, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	var costs query.IteratorCost
	for _, r := range results {
		if r.err != nil {
			return costs, r.err
		}
		costs = costs.Combine(r.cost)
	}
	return costs, nil
}

func (me *ClusterExecutor) FieldDimensions(m *influxql.Measurement, shards []meta.ShardInfo) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	type Result struct {
		fields     map[string]influxql.DataType
		dimensions map[string]struct{}
		err        error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var fields map[string]influxql.DataType
			var dimensions map[string]struct{}
			var err error
			if nodeId == me.Node.ID {
				sg := me.TSDBStore.ShardGroup(shardIDs)
				var measurements []string
				if m.Regex != nil {
					measurements = sg.MeasurementsByRegex(m.Regex.Val)
				} else {
					measurements = []string{m.Name}
				}
				fields, dimensions, err = sg.FieldDimensions(measurements)
			} else {
				fields, dimensions, err = me.RemoteNodeExecutor.FieldDimensions(nodeId, m, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{fields: fields, dimensions: dimensions, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	for _, r := range results {
		//TODO: merge err
		if r.err != nil {
			return nil, nil, r.err
		}

		for f, t := range r.fields {
			fields[f] = t
		}
		for d, _ := range r.dimensions {
			dimensions[d] = struct{}{}
		}
	}

	return fields, dimensions, nil
}

func GetShardInfoByIds(MetaClient interface {
	ShardOwner(id uint64) (string, string, *meta.ShardGroupInfo)
}, ids []uint64) ([]meta.ShardInfo, error) {
	var shards []meta.ShardInfo
	for _, id := range ids {
		_, _, sgi := MetaClient.ShardOwner(id)
		if sgi == nil {
			return nil, fmt.Errorf("not find shard %d", id)
		}
		for _, shard := range sgi.Shards {
			if shard.ID == id {
				shards = append(shards, shard)
				break
			}
		}
	}
	return shards, nil
}

func (me *ClusterExecutor) TagKeys(auth query.Authorizer, ids []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	type TagKeysResult struct {
		keys []tsdb.TagKeys
		err  error
	}

	shards, err := GetShardInfoByIds(me.MetaClient, ids)
	if err != nil {
		return nil, err
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	result := make(map[uint64]*TagKeysResult)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var tagKeys []tsdb.TagKeys
			var err error
			if nodeId == me.Node.ID {
				tagKeys, err = me.TSDBStore.TagKeys(auth, shardIDs, cond)
			} else {
				tagKeys, err = me.RemoteNodeExecutor.TagKeys(nodeId, shardIDs, cond)
			}

			if err != nil {
				me.Logger.Error("TagKeys fail", zap.Error(err), zap.Uint64("node", nodeId))
			}
			mutex.Lock()
			result[nodeId] = &TagKeysResult{keys: tagKeys, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	uniqKeys := make(map[string]map[string]struct{})
	for _, r := range result {
		if r.err != nil {
			return nil, r.err
		}

		for _, tagKey := range r.keys {
			m, ok := uniqKeys[tagKey.Measurement]
			if !ok {
				m = make(map[string]struct{})
			}
			for _, key := range tagKey.Keys {
				m[key] = struct{}{}
			}
			uniqKeys[tagKey.Measurement] = m
		}
	}

	tagKeys := make([]tsdb.TagKeys, len(uniqKeys))
	idx := 0
	for m, keys := range uniqKeys {
		tagKey := &tagKeys[idx]
		tagKey.Measurement = m
		for k, _ := range keys {
			tagKey.Keys = append(tagKey.Keys, k)
		}
		sort.Sort(StringSlice(tagKey.Keys))
		idx = idx + 1
	}

	sort.Sort(tsdb.TagKeysSlice(tagKeys))
	return tagKeys, nil
}

func (me *ClusterExecutor) DeleteMeasurement(database, name string) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				err = me.TSDBStore.DeleteMeasurement(database, name)
			} else {
				err = me.RemoteNodeExecutor.DeleteMeasurement(nodeId, database, name)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutor) DeleteDatabase(database string) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				err = me.TSDBStore.DeleteDatabase(database)
			} else {
				err = me.RemoteNodeExecutor.DeleteDatabase(nodeId, database)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutor) DeleteSeries(database string, sources []influxql.Source, cond influxql.Expr) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				// Convert "now()" to current time.
				cond = influxql.Reduce(cond, &influxql.NowValuer{Now: time.Now().UTC()})
				err = me.TSDBStore.DeleteSeries(database, sources, cond)
			} else {
				err = me.RemoteNodeExecutor.DeleteSeries(nodeId, database, sources, cond)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

type StringSlice []string

func (a StringSlice) Len() int           { return len(a) }
func (a StringSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a StringSlice) Less(i, j int) bool { return a[i] < a[j] }

type NodeIds []uint64

func NewNodeIdsByNodes(nodeInfos []meta.NodeInfo) NodeIds {
	var ids []uint64
	for _, ni := range nodeInfos {
		ids = append(ids, ni.ID)
	}
	return NodeIds(ids)
}

//TODO:取个达意的名字
func NewNodeIdsByShards(Shards []meta.ShardInfo) NodeIds {
	m := make(map[uint64]struct{})
	for _, si := range Shards {
		for _, owner := range si.Owners {
			m[owner.NodeID] = struct{}{}
		}
	}

	nodes := make([]uint64, 0, len(m))
	for n, _ := range m {
		nodes = append(nodes, n)
	}
	return nodes
}

func (me NodeIds) Apply(fn func(nodeId uint64)) {
	for _, nodeID := range me {
		fn(nodeID)
	}
}

type Node2ShardIDs map[uint64][]uint64

func NewNode2ShardIDs(mc interface {
	DataNode(nodeId uint64) (*meta.NodeInfo, error)
},
	localNode *influxdb.Node,
	shards []meta.ShardInfo) Node2ShardIDs {
	allNodes := make([]uint64, 0)
	for _, si := range shards {
		if si.OwnedBy(localNode.ID) {
			continue
		}

		for _, owner := range si.Owners {
			allNodes = append(allNodes, owner.NodeID)
		}
	}

	//选出 active node
	activeNodes := make(map[uint64]struct{})
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for _, id := range allNodes {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Add(-1)
			dialer := &NodeDialer{
				MetaClient: mc,
				Timeout:    100 * time.Millisecond, //TODO: from config
			}

			conn, err := dialer.DialNode(id)
			if err != nil {
				return
			}
			defer conn.Close()
			mutex.Lock()
			activeNodes[id] = struct{}{}
			mutex.Unlock()
		}(id)
	}

	wg.Wait()

	shardIDsByNodeID := make(map[uint64][]uint64)
	for _, si := range shards {
		var nodeID uint64
		if si.OwnedBy(localNode.ID) {
			nodeID = localNode.ID
		} else if len(si.Owners) > 0 {
			nodeID = si.Owners[rand.Intn(len(si.Owners))].NodeID
			if _, ok := activeNodes[nodeID]; !ok {
				//利用map的顺序不确定特性，随机选一个active的owner
				randomOwners := make(map[uint64]struct{})
				for _, owner := range si.Owners {
					randomOwners[owner.NodeID] = struct{}{}
				}
				for id, _ := range randomOwners {
					if _, ok := activeNodes[id]; ok {
						nodeID = id
						break
					}
				}
			}
		} else {
			continue
		}
		shardIDsByNodeID[nodeID] = append(shardIDsByNodeID[nodeID], si.ID)
	}

	return shardIDsByNodeID
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

func (me Node2ShardIDs) Apply(fn func(nodeId uint64, shardIDs []uint64)) {
	for nodeID, shardIDs := range me {
		// Sort shard IDs so we get more predicable execution.
		sort.Sort(uint64Slice(shardIDs))
		fn(nodeID, shardIDs)
	}
}
