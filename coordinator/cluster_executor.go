package coordinator

import (
	"context"
	"fmt"
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

func NewClusterExecutor(n *influxdb.Node, s TSDBStore, m MetaClient, pool *ClientPool, Config Config) *ClusterExecutor {
	executor := &ClusterExecutor{
		Node:       n,
		TSDBStore:  s,
		MetaClient: m,
		RemoteNodeExecutor: &remoteNodeExecutor{
			ClientPool:         pool,
			DailTimeout:        time.Duration(Config.DailTimeout),
			ShardReaderTimeout: time.Duration(Config.ShardReaderTimeout),
			ClusterTracing:     Config.ClusterTracing,
		},
		Logger: zap.NewNop(),
	}
	executor.RemoteNodeExecutor.WithLogger(executor.Logger)
	return executor
}

func (me *ClusterExecutor) WithLogger(log *zap.Logger) {
	me.Logger = log.With(zap.String("service", "ClusterExecutor"))
	me.RemoteNodeExecutor.WithLogger(log)
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

	nodes := toNodeIds(nodeInfos)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
LOOP:
	for _, nodeId := range nodes {
		host := "unkown"
		node, _ := me.MetaClient.DataNode(nodeId)
		if node != nil {
			host = node.Host
		}

		switch t := stmt.(type) {
		case *influxql.KillQueryStatement:
			if t.Host != "" && t.Host != host {
				// not this node
				continue LOOP
			}
		}

		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var err error
			var qr *query.Result
			if curNodeId == me.Node.ID {
				recvCtx := &query.ExecutionContext{
					Context: context.Background(),
					Results: make(chan *query.Result, 1),
				}
				err = me.TaskManager.ExecuteStatement(stmt, recvCtx)
				if err == nil {
					qr = <-recvCtx.Results
				}
			} else {
				qr, err = me.RemoteNodeExecutor.TaskManagerStatement(curNodeId, stmt)
			}

			if qr != nil && len(qr.Series) > 0 {
				qr.Series[0].Columns = append(qr.Series[0].Columns, "host")
				for i := 0; i < len(qr.Series[0].Values); i++ {
					qr.Series[0].Values[i] = append(qr.Series[0].Values[i], host)
				}
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{qr: qr, err: err}
		}(nodeId)
	}
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
	nodes := getAllRelatedNodes(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, nodeId := range nodes {
		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var n int64
			var err error
			if curNodeId == me.Node.ID {
				n, err = me.TSDBStore.SeriesCardinality(database)
			} else {
				n, err = me.RemoteNodeExecutor.SeriesCardinality(curNodeId, database)
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{n: n, err: err}
		}(nodeId)
	}
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
	nodes := getAllRelatedNodes(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, nodeId := range nodes {
		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var names [][]byte
			var err error
			if curNodeId == me.Node.ID {
				names, err = me.TSDBStore.MeasurementNames(auth, database, cond)
			} else {
				names, err = me.RemoteNodeExecutor.MeasurementNames(curNodeId, database, cond)
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{names: names, err: err}
		}(nodeId)
	}
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
	for name := range uniq {
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

	n2s := PlanNodes(me.Node.ID, shards, nil)

	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		var tagValues []tsdb.TagValues
		shardIDs := toShardIDs(shards)
		if nodeId == me.Node.ID {
			tagValues, err = me.TSDBStore.TagValues(auth, shardIDs, cond)
		} else {
			tagValues, err = me.RemoteNodeExecutor.TagValues(nodeId, shardIDs, cond)
		}
		result = &TagValuesResult{values: tagValues, err: err}
		return
	}
	result, err := n2s.ExecuteWithRetry(fn)

	extractKeyFn := func(kv tsdb.KeyValue) string {
		//TODO:
		return fmt.Sprintf("key=%s+-*/value=%s", kv.Key, kv.Value)
	}

	uniq := make(map[string]map[string]tsdb.KeyValue)
	for nodeId, t := range result {
		r, ok := t.(*TagValuesResult)
		if !ok {
			continue
		}
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

func (me *ClusterExecutor) createLocalIteratorfunc(
	localCtx context.Context,
	m *influxql.Measurement,
	shardIDs []uint64,
	opt query.IteratorOptions,
) (itr query.Iterator, err error) {
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
}

func (me *ClusterExecutor) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.Iterator, error) {
	type Result struct {
		iter query.Iterator
		err  error
	}

	n2s := PlanNodes(me.Node.ID, shards, nil)

	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		var iter query.Iterator
		shardIDs := toShardIDs(shards)
		if nodeId == me.Node.ID {
			//localCtx only use for local node
			iter, err = me.createLocalIteratorfunc(ctx, m, shardIDs, opt)
		} else {
			iter, err = me.RemoteNodeExecutor.CreateIterator(nodeId, ctx, m, opt, shardIDs)
		}

		result = &Result{iter: iter, err: err}
		return
	}
	result, _ := n2s.ExecuteWithRetry(fn)

	seriesN := 0
	var errOccur error
	inputs := make([]query.Iterator, 0, len(result))
	for _, t := range result {
		r, ok := t.(*Result)
		if !ok {
			continue
		}
		if r.err != nil {
			errOccur = r.err
			break
		}
		if r.iter != nil {
			stats := r.iter.Stats()
			seriesN += stats.SeriesN
			inputs = append(inputs, r.iter)
		}
	}
	if errOccur != nil {
		// close all iterators
		for _, t := range result {
			r, ok := t.(*Result)
			if !ok {
				continue
			}
			if r.iter != nil {
				r.iter.Close()
			}
		}
		return nil, errOccur
	}

	if opt.MaxSeriesN > 0 && seriesN > opt.MaxSeriesN {
		query.Iterators(inputs).Close()
		return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", seriesN, opt.MaxSeriesN)
	}
	return query.Iterators(inputs).Merge(opt)
}

func (me *ClusterExecutor) MapType(m *influxql.Measurement, field string, shards []meta.ShardInfo) influxql.DataType {
	type Result struct {
		nodeId   uint64
		dataType influxql.DataType
		err      error
	}

	n2s := PlanNodes(me.Node.ID, shards, nil)
	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		shardIDs := toShardIDs(shards)
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
			result = &Result{dataType: typ, err: nil, nodeId: nodeId}
		}
		return
	}
	result, _ := n2s.ExecuteWithRetry(fn)
	for _, t := range result {
		if t == nil {
			continue
		}
		r, ok := t.(*Result)
		if !ok {
			continue
		}
		if r.dataType != influxql.Unknown {
			return r.dataType
		}
	}

	//本地失败, 尝试仅从remote node获取
	fn = func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		shardIDs := toShardIDs(shards)
		if nodeId != me.Node.ID {
			typ, err := me.RemoteNodeExecutor.MapType(nodeId, m, field, shardIDs)
			result = &Result{dataType: typ, err: err}
		}
		return
	}
	result, _ = n2s.ExecuteWithRetry(fn)

	typ := influxql.Unknown
	for _, t := range result {
		r, ok := t.(*Result)
		if !ok {
			continue
		}
		if r.err != nil {
			me.Logger.Warn("results have error", zap.Error(r.err), zap.Uint64("node", r.nodeId))
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

	n2s := PlanNodes(me.Node.ID, shards, nil)

	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		shardIDs := toShardIDs(shards)
		var cost query.IteratorCost
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

		result = &Result{cost: cost, err: err}
		return
	}
	result, _ := n2s.ExecuteWithRetry(fn)

	var costs query.IteratorCost
	for _, t := range result {
		r, ok := t.(*Result)
		if !ok {
			continue
		}
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

	n2s := PlanNodes(me.Node.ID, shards, nil)

	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		var fields map[string]influxql.DataType
		var dimensions map[string]struct{}
		shardIDs := toShardIDs(shards)
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
		if fields != nil {
			result = &Result{fields: fields, dimensions: dimensions, err: err}
		}
		return
	}

	result, err := n2s.ExecuteWithRetry(fn)

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	for _, t := range result {
		r, ok := t.(*Result)
		if !ok {
			continue
		}
		//TODO: merge err
		if r.err != nil {
			return nil, nil, r.err
		}

		for f, t := range r.fields {
			fields[f] = t
		}
		for d := range r.dimensions {
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

	n2s := PlanNodes(me.Node.ID, shards, nil)

	fn := func(nodeId uint64, shards []meta.ShardInfo) (result interface{}, err error) {
		var tagKeys []tsdb.TagKeys
		shardIDs := toShardIDs(shards)
		if nodeId == me.Node.ID {
			tagKeys, err = me.TSDBStore.TagKeys(auth, shardIDs, cond)
		} else {
			tagKeys, err = me.RemoteNodeExecutor.TagKeys(nodeId, shardIDs, cond)
		}

		if err != nil {
			me.Logger.Error("TagKeys fail", zap.Error(err), zap.Uint64("node", nodeId))
		}
		if len(tagKeys) > 0 {
			result = &TagKeysResult{keys: tagKeys, err: err}
		}
		return
	}

	result, err := n2s.ExecuteWithRetry(fn)

	uniqKeys := make(map[string]map[string]struct{})
	for _, t := range result {
		r, ok := t.(*TagKeysResult)
		if !ok {
			continue
		}
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
		for k := range keys {
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
	nodes := getAllRelatedNodes(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, nodeId := range nodes {
		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var err error
			if curNodeId == me.Node.ID {
				err = me.TSDBStore.DeleteMeasurement(database, name)
			} else {
				err = me.RemoteNodeExecutor.DeleteMeasurement(curNodeId, database, name)
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{err: err}
		}(nodeId)
	}
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
	nodes := getAllRelatedNodes(shards)
	results := make(map[uint64]*Result, len(nodes))

	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, nodeId := range nodes {
		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var err error
			if curNodeId == me.Node.ID {
				err = me.TSDBStore.DeleteDatabase(database)
			} else {
				err = me.RemoteNodeExecutor.DeleteDatabase(curNodeId, database)
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{err: err}
			return
		}(nodeId)
	}
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
	nodes := getAllRelatedNodes(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, nodeId := range nodes {
		wg.Add(1)
		go func(curNodeId uint64) {
			defer wg.Done()

			var err error
			if curNodeId == me.Node.ID {
				// Convert "now()" to current time.
				cond = influxql.Reduce(cond, &influxql.NowValuer{Now: time.Now().UTC()})
				err = me.TSDBStore.DeleteSeries(database, sources, cond)
			} else {
				err = me.RemoteNodeExecutor.DeleteSeries(curNodeId, database, sources, cond)
			}

			mutex.Lock()
			defer mutex.Unlock()
			results[curNodeId] = &Result{err: err}
		}(nodeId)
	}
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

func toNodeIds(nodeInfos []meta.NodeInfo) []uint64 {
	ids := make([]uint64, len(nodeInfos))
	for i, ni := range nodeInfos {
		ids[i] = ni.ID
	}
	return ids
}

// getAllRelatedNodes returns all related nodes owning shards
func getAllRelatedNodes(shards []meta.ShardInfo) []uint64 {
	nodeMap := make(map[uint64]bool)
	for _, shard := range shards {
		for _, n := range shard.Owners {
			nodeMap[n.NodeID] = true
		}
	}

	ids := make([]uint64, 0, len(nodeMap))
	for id := range nodeMap {
		ids = append(ids, id)
	}
	return ids
}

func toShardIDs(shards []meta.ShardInfo) []uint64 {
	ids := make([]uint64, len(shards))
	for i, shard := range shards {
		ids[i] = shard.ID
	}
	return ids
}
