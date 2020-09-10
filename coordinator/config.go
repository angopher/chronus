// Package coordinator contains abstractions for writing points, executing statements,
// and accessing meta data.
package coordinator

import (
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 10 * time.Second

	DefaultDialTimeout = 1 * time.Second

	DefaultPoolMaxIdleTimeout = 60 * time.Second
	DefaultShardReaderTimeout = 600 * time.Second

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	DefaultPoolMinStreamsPerNode = 5
	DefaultPoolMaxStreamsPerNode = 200

	// DefaultMaxSelectPointN is the maximum number of points a SELECT can process.
	// A value of zero will make the maximum point count unlimited.
	DefaultMaxSelectPointN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectSeriesN = 0

	DefaultMetaService = "127.0.0.1:2347"
)

// Config represents the configuration for the coordinator service.
type Config struct {
	DailTimeout               toml.Duration `toml:"dial-timeout"`
	PoolMaxIdleTimeout        toml.Duration `toml:"pool-max-idle-time"`
	PoolMinStreamsPerNode     int           `toml:"pool-min-streams-per-node"`
	PoolMaxStreamsPerNode     int           `toml:"pool-max-streams-per-node"`
	ShardReaderTimeout        toml.Duration `toml:"shard-reader-timeout"`
	ClusterTracing            bool          `toml:"cluster-tracing"`
	WriteTimeout              toml.Duration `toml:"write-timeout"`
	MaxConcurrentQueries      int           `toml:"max-concurrent-queries"`
	QueryTimeout              toml.Duration `toml:"query-timeout"`
	LogQueriesAfter           toml.Duration `toml:"log-queries-after"`
	MaxSelectPointN           int           `toml:"max-select-point"`
	MaxSelectSeriesN          int           `toml:"max-select-series"`
	MaxSelectBucketsN         int           `toml:"max-select-buckets"`
	MetaServices              []string      `toml:"meta-services"`
	PingMetaServiceIntervalMs int64         `toml:"ping-meta-service-interval"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		DailTimeout:               toml.Duration(DefaultDialTimeout),
		PoolMaxIdleTimeout:        toml.Duration(DefaultPoolMaxIdleTimeout),
		PoolMinStreamsPerNode:     DefaultPoolMinStreamsPerNode,
		PoolMaxStreamsPerNode:     DefaultPoolMaxStreamsPerNode,
		ShardReaderTimeout:        toml.Duration(DefaultShardReaderTimeout),
		ClusterTracing:            false,
		WriteTimeout:              toml.Duration(DefaultWriteTimeout),
		QueryTimeout:              toml.Duration(query.DefaultQueryTimeout),
		MaxConcurrentQueries:      DefaultMaxConcurrentQueries,
		MaxSelectPointN:           DefaultMaxSelectPointN,
		MaxSelectSeriesN:          DefaultMaxSelectSeriesN,
		MetaServices:              []string{DefaultMetaService},
		PingMetaServiceIntervalMs: 50,
	}
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"dail-timeout":               c.DailTimeout,
		"pool-max-idle-time":         c.PoolMaxIdleTimeout,
		"pool-min-streams-per-node":  c.PoolMinStreamsPerNode,
		"pool-max-streams-per-node":  c.PoolMaxStreamsPerNode,
		"shard-reader-timeout":       c.ShardReaderTimeout,
		"cluster-tracing":            c.ClusterTracing,
		"write-timeout":              c.WriteTimeout,
		"max-concurrent-queries":     c.MaxConcurrentQueries,
		"query-timeout":              c.QueryTimeout,
		"log-queries-after":          c.LogQueriesAfter,
		"max-select-point":           c.MaxSelectPointN,
		"max-select-series":          c.MaxSelectSeriesN,
		"max-select-buckets":         c.MaxSelectBucketsN,
		"meta-services":              c.MetaServices,
		"ping-meta-service-interval": c.PingMetaServiceIntervalMs,
	}), nil
}
