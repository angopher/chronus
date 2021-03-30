package internal

import (
	"sync/atomic"

	"github.com/influxdata/influxdb/models"
)

// Statistics maintained by the cluster package
const (
	writeShardReq       = "writeShardReq"
	writeShardPointsReq = "writeShardPointsReq"
	writeShardFail      = "writeShardFail"

	createIteratorReq  = "createIteratorReq"
	createIteratorFail = "createIteratorFail"

	fieldDimensionsReq  = "fieldDimensionsReq"
	fieldDimensionsFail = "fieldDimensionsFail"

	tagKeysReq  = "tagKeysReq"
	tagKeysFail = "tagKeysFail"

	tagValuesReq  = "tagValuesReq"
	tagValuesFail = "tagValuesFail"

	measurementNamesReq  = "measurementNamesReq"
	measurementNamesFail = "measurementNamesFail"

	seriesCardinalityReq  = "seriesCardinalityReq"
	seriesCardinalityFail = "seriesCardinalityFail"

	iteratorCostReq  = "iteratorCostReq"
	iteratorCostFail = "iteratorCostFail"

	mapTypeReq  = "mapTypeReq"
	mapTypeFail = "mapTypeFail"
)

type InternalServiceStatistics struct {
	WriteShardReq       int64
	WriteShardPointsReq int64
	WriteShardFail      int64

	CreateIteratorReq  int64
	CreateIteratorFail int64

	FieldDimensionsReq  int64
	FieldDimensionsFail int64

	TagKeysReq  int64
	TagKeysFail int64

	TagValuesReq  int64
	TagValuesFail int64

	MeasurementNamesReq  int64
	MeasurementNamesFail int64

	SeriesCardinalityReq  int64
	SeriesCardinalityFail int64

	IteratorCostReq  int64
	IteratorCostFail int64

	MapTypeReq  int64
	MapTypeFail int64
}

func Statistics(stats *InternalServiceStatistics, tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "coordinator_service",
		Tags: tags,
		Values: map[string]interface{}{
			writeShardReq:       atomic.LoadInt64(&stats.WriteShardReq),
			writeShardPointsReq: atomic.LoadInt64(&stats.WriteShardPointsReq),
			writeShardFail:      atomic.LoadInt64(&stats.WriteShardFail),

			createIteratorReq:  atomic.LoadInt64(&stats.CreateIteratorReq),
			createIteratorFail: atomic.LoadInt64(&stats.CreateIteratorFail),

			fieldDimensionsReq:  atomic.LoadInt64(&stats.FieldDimensionsReq),
			fieldDimensionsFail: atomic.LoadInt64(&stats.FieldDimensionsFail),

			tagKeysReq:  atomic.LoadInt64(&stats.TagKeysReq),
			tagKeysFail: atomic.LoadInt64(&stats.TagKeysFail),

			tagValuesReq:  atomic.LoadInt64(&stats.TagValuesReq),
			tagValuesFail: atomic.LoadInt64(&stats.TagValuesFail),

			measurementNamesReq:  atomic.LoadInt64(&stats.MeasurementNamesReq),
			measurementNamesFail: atomic.LoadInt64(&stats.MeasurementNamesFail),

			seriesCardinalityReq:  atomic.LoadInt64(&stats.SeriesCardinalityReq),
			seriesCardinalityFail: atomic.LoadInt64(&stats.SeriesCardinalityFail),

			iteratorCostReq:  atomic.LoadInt64(&stats.IteratorCostReq),
			iteratorCostFail: atomic.LoadInt64(&stats.IteratorCostFail),

			mapTypeReq:  atomic.LoadInt64(&stats.MapTypeReq),
			mapTypeFail: atomic.LoadInt64(&stats.MapTypeFail),
		},
	}}
}
