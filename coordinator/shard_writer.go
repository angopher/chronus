package coordinator

import (
	"fmt"
	"time"

	"github.com/angopher/chronus/coordinator/request"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

const (
	writeShardRequestMessage byte = iota + 1
	writeShardResponseMessage

	executeStatementRequestMessage
	executeStatementResponseMessage

	createIteratorRequestMessage
	createIteratorResponseMessage

	fieldDimensionsRequestMessage
	fieldDimensionsResponseMessage

	seriesKeysRequestMessage
	seriesKeysResponseMessage

	tagKeysRequestMessage
	tagKeysResponseMessage

	tagValuesRequestMessage
	tagValuesResponseMessage

	measurementNamesRequestMessage
	measurementNamesResponseMessage

	seriesCardinalityRequestMessage
	seriesCardinalityResponseMessage

	deleteSeriesRequestMessage
	deleteSeriesResponseMessage

	deleteDatabaseRequestMessage
	deleteDatabaseResponseMessage

	deleteMeasurementRequestMessage
	deleteMeasurementResponseMessage

	iteratorCostRequestMessage
	iteratorCostResponseMessage

	mapTypeRequestMessage
	mapTypeResponseMessage

	executeTaskManagerRequestMessage
	executeTaskManagerResponseMessage

	testRequestMessage // one way message
)

// ShardWriter writes a set of points to a shard.
type ShardWriter struct {
	pool    *ClientPool
	timeout time.Duration
	logger  *zap.Logger

	MetaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
	}
}

// NewShardWriter returns a new instance of ShardWriter.
func NewShardWriter(timeout time.Duration, pool *ClientPool) *ShardWriter {
	return &ShardWriter{
		pool:    pool,
		timeout: timeout,
		logger:  zap.NewNop(),
	}
}

func (w *ShardWriter) WithLogger(logger *zap.Logger) {
	w.logger = logger.With(zap.String("service", "ShardWriter"))
}

// WriteShard writes time series points to a shard
func (w *ShardWriter) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	conn, err := getConnWithRetry(w.pool, ownerID, w.logger)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Determine the location of this shard and whether it still exists
	db, rp, sgi := w.MetaClient.ShardOwner(shardID)
	if sgi == nil {
		// If we can't get the shard group for this shard, then we need to drop this request
		// as it is no longer valid.  This could happen if writes were queued via
		// hinted handoff and we're processing the queue after a shard group was deleted.
		return nil
	}

	// Build write writeReq.
	var writeReq WriteShardRequest
	writeReq.SetShardID(shardID)
	writeReq.SetDatabase(db)
	writeReq.SetRetentionPolicy(rp)
	writeReq.AddPoints(points)

	// Marshal into protocol buffers.
	buf, err := writeReq.MarshalBinary()
	if err != nil {
		return err
	}

	// Write request.
	conn.SetWriteDeadline(time.Now().Add(w.timeout))
	if err := WriteTLV(conn, writeShardRequestMessage, buf); err != nil {
		conn.MarkUnusable()
		return err
	}

	// Read the response.
	requestReader := &request.ClusterMessageReader{}
	conn.SetReadDeadline(time.Now().Add(w.timeout))
	resp, err := requestReader.Read(conn)
	if err != nil {
		conn.MarkUnusable()
		return err
	}
	conn.SetDeadline(time.Time{})

	// Unmarshal response.
	var response WriteShardResponse
	if err := response.UnmarshalBinary(resp.Data); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

// Close closes ShardWriter's pool
func (w *ShardWriter) Close() error {
	if w.pool == nil {
		return fmt.Errorf("client already closed")
	}
	w.pool.close()
	w.pool = nil
	return nil
}

func (w *ShardWriter) Stats() []StatEntity {
	return w.pool.Stat()
}
