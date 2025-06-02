package storage

import (
	"context"
	"errors"
	"sort"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
)

var (
	errCurrentBoltdbShipperNon24Hours  = errors.New("boltdb-shipper works best with 24h periodic index config. Either add a new config with future date set to 24h to retain the existing index or change the existing config to use 24h period")
	errUpcomingBoltdbShipperNon24Hours = errors.New("boltdb-shipper with future date must always have periodic config for index set to 24h")
	errZeroLengthConfig                = errors.New("must specify at least one schema configuration")
)

type CortexStoreCommon interface {
	Put(ctx context.Context, chunks []chunk.Chunk) error
	PutOne(ctx context.Context, from, through model.Time, chunk chunk.Chunk) error
	Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error)
	// GetChunkRefs returns the un-loaded chunks and the fetchers to be used to load them. You can load each slice of chunks ([]Chunk),
	// using the corresponding Fetcher (fetchers[i].FetchChunks(ctx, chunks[i], ...)
	GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]chunk.Chunk, []*chunk.Fetcher, error)
	GetChunkFetcher(tm model.Time) *chunk.Fetcher

	// DeleteChunk deletes a chunks index entry and then deletes the actual chunk from chunk storage.
	// It takes care of chunks which are deleting partially by creating and inserting a new chunk first and then deleting the original chunk
	DeleteChunk(ctx context.Context, from, through model.Time, userID, chunkID string, metric labels.Labels, partiallyDeletedInterval *model.Interval) error
	// DeleteSeriesIDs is only relevant for SeriesStore.
	DeleteSeriesIDs(ctx context.Context, from, through model.Time, userID string, metric labels.Labels) error
	Stop()
}

// Store is the Loki chunk store to retrieve and save chunks.
type Store interface {
	LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string, matchers ...*labels.Matcher) ([]string, error)
	LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, matchers ...*labels.Matcher) ([]string, error)

	SelectSamples(ctx context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error)
	SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error)
	GetSeries(ctx context.Context, req logql.SelectLogParams) ([]logproto.SeriesIdentifier, error)
	GetSchemaConfigs() []chunk.PeriodConfig
}

// ActivePeriodConfig returns index of active PeriodicConfig which would be applicable to logs that would be pushed starting now.
// Note: Another PeriodicConfig might be applicable for future logs which can change index type.
func ActivePeriodConfig(configs []chunk.PeriodConfig) int {
	now := model.Now()
	i := sort.Search(len(configs), func(i int) bool {
		return configs[i].From.Time > now
	})
	if i > 0 {
		i--
	}
	return i
}
