package storage

import (
	"context"
	"errors"
	"flag"
	"sort"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/user"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
	"github.com/ronanh/loki/logql/stats"
	"github.com/ronanh/loki/storage/stores/shipper"
	"github.com/ronanh/loki/util"
)

var (
	errCurrentBoltdbShipperNon24Hours  = errors.New("boltdb-shipper works best with 24h periodic index config. Either add a new config with future date set to 24h to retain the existing index or change the existing config to use 24h period")
	errUpcomingBoltdbShipperNon24Hours = errors.New("boltdb-shipper with future date must always have periodic config for index set to 24h")
	errZeroLengthConfig                = errors.New("must specify at least one schema configuration")
)

// Config is the loki storage configuration
type Config struct {
	storage.Config    `yaml:",inline"`
	MaxChunkBatchSize int `yaml:"max_chunk_batch_size"`
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Config.RegisterFlags(f)
	f.IntVar(&cfg.MaxChunkBatchSize, "store.max-chunk-batch-size", 50, "The maximum number of chunks to fetch per batch.")
}

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	chunk.SchemaConfig `yaml:",inline"`
}

// Validate the schema config and returns an error if the validation doesn't pass
func (cfg *SchemaConfig) Validate() error {
	if len(cfg.Configs) == 0 {
		return errZeroLengthConfig
	}
	activePCIndex := ActivePeriodConfig((*cfg).Configs)

	// if current index type is boltdb-shipper and there are no upcoming index types then it should be set to 24 hours.
	if cfg.Configs[activePCIndex].IndexType == shipper.BoltDBShipperType && cfg.Configs[activePCIndex].IndexTables.Period != 24*time.Hour && len(cfg.Configs)-1 == activePCIndex {
		return errCurrentBoltdbShipperNon24Hours
	}

	// if upcoming index type is boltdb-shipper, it should always be set to 24 hours.
	if len(cfg.Configs)-1 > activePCIndex && (cfg.Configs[activePCIndex+1].IndexType == shipper.BoltDBShipperType && cfg.Configs[activePCIndex+1].IndexTables.Period != 24*time.Hour) {
		return errUpcomingBoltdbShipperNon24Hours
	}

	return cfg.SchemaConfig.Validate()
}

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
	// chunk.Store // remove dependency on chunk.Store (cortex)
	CortexStoreCommon

	LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string, matchers ...*labels.Matcher) ([]string, error)
	LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, matchers ...*labels.Matcher) ([]string, error)

	SelectSamples(ctx context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error)
	SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error)
	GetSeries(ctx context.Context, req logql.SelectLogParams) ([]logproto.SeriesIdentifier, error)
	GetSchemaConfigs() []chunk.PeriodConfig
}

type store struct {
	chunk.Store
	cfg          Config
	chunkMetrics *ChunkMetrics
	schemaCfg    SchemaConfig
}

// NewStore creates a new Loki Store using configuration supplied.
func NewStore(cfg Config, schemaCfg SchemaConfig, chunkStore chunk.Store, registerer prometheus.Registerer) (Store, error) {
	return &store{
		Store:        chunkStore,
		cfg:          cfg,
		chunkMetrics: NewChunkMetrics(registerer, cfg.MaxChunkBatchSize),
		schemaCfg:    schemaCfg,
	}, nil
}

// decodeReq sanitizes an incoming request, rounds bounds, appends the __name__ matcher,
// and adds the "__cortex_shard__" label if this is a sharded query.
// todo(cyriltovena) refactor this.
func decodeReq(req logql.QueryParams) ([]*labels.Matcher, model.Time, model.Time, error) {
	expr, err := req.LogSelector()
	if err != nil {
		return nil, 0, 0, err
	}

	matchers := expr.Matchers()
	nameLabelMatcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, "logs")
	if err != nil {
		return nil, 0, 0, err
	}
	matchers = append(matchers, nameLabelMatcher)

	if shards := req.GetShards(); shards != nil {
		parsed, err := logql.ParseShards(shards)
		if err != nil {
			return nil, 0, 0, err
		}
		for _, s := range parsed {
			shardMatcher, err := labels.NewMatcher(
				labels.MatchEqual,
				astmapper.ShardLabel,
				s.String(),
			)
			if err != nil {
				return nil, 0, 0, err
			}
			matchers = append(matchers, shardMatcher)

			// TODO(owen-d): passing more than one shard will require
			// a refactor to cortex to support it. We're leaving this codepath in
			// preparation of that but will not pass more than one until it's supported.
			break // nolint:staticcheck
		}
	}

	from, through := util.RoundToMilliseconds(req.GetStart(), req.GetEnd())
	return matchers, from, through, nil
}

// lazyChunks is an internal function used to resolve a set of lazy chunks from the store without actually loading them. It's used internally by `LazyQuery` and `GetSeries`
func (s *store) lazyChunks(ctx context.Context, matchers []*labels.Matcher, from, through model.Time) ([]*LazyChunk, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	storeStats := stats.GetStoreData(ctx)

	chks, fetchers, err := s.GetChunkRefs(ctx, userID, from, through, matchers...)
	if err != nil {
		return nil, err
	}

	var prefiltered int
	var filtered int
	for i := range chks {
		prefiltered += len(chks[i])
		storeStats.TotalChunksRef += int64(len(chks[i]))
		chks[i] = filterChunksByTime(from, through, chks[i])
		filtered += len(chks[i])
	}

	s.chunkMetrics.refs.WithLabelValues(statusDiscarded).Add(float64(prefiltered - filtered))
	s.chunkMetrics.refs.WithLabelValues(statusMatched).Add(float64(filtered))

	// creates lazychunks with chunks ref.
	lazyChunks := make([]*LazyChunk, 0, filtered)
	for i := range chks {
		for _, c := range chks[i] {
			lazyChunks = append(lazyChunks, &LazyChunk{Chunk: c, Fetcher: fetchers[i]})
		}
	}
	return lazyChunks, nil
}

func (s *store) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string, matchers ...*labels.Matcher) ([]string, error) {
	return s.Store.LabelValuesForMetricName(ctx, userID, from, through, metricName, labelName)
}

func (s *store) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, matchers ...*labels.Matcher) ([]string, error) {
	return s.Store.LabelNamesForMetricName(ctx, userID, from, through, metricName)
}

func (s *store) GetSeries(ctx context.Context, req logql.SelectLogParams) ([]logproto.SeriesIdentifier, error) {
	var from, through model.Time
	var matchers []*labels.Matcher

	// The Loki parser doesn't allow for an empty label matcher but for the Series API
	// we allow this to select all series in the time range.
	if req.Selector == "" {
		from, through = util.RoundToMilliseconds(req.Start, req.End)
		nameLabelMatcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, "logs")
		if err != nil {
			return nil, err
		}
		matchers = []*labels.Matcher{nameLabelMatcher}
	} else {
		var err error
		matchers, from, through, err = decodeReq(req)
		if err != nil {
			return nil, err
		}
	}

	lazyChunks, err := s.lazyChunks(ctx, matchers, from, through)
	if err != nil {
		return nil, err
	}

	// group chunks by series
	chunksBySeries := partitionBySeriesChunks(lazyChunks)

	firstChunksPerSeries := make([]*LazyChunk, 0, len(chunksBySeries))

	// discard all but one chunk per series
	for _, chks := range chunksBySeries {
		firstChunksPerSeries = append(firstChunksPerSeries, chks[0][0])
	}

	results := make(logproto.SeriesIdentifiers, 0, len(firstChunksPerSeries))

	// bound concurrency
	groups := make([][]*LazyChunk, 0, len(firstChunksPerSeries)/s.cfg.MaxChunkBatchSize+1)

	split := min(len(firstChunksPerSeries), s.cfg.MaxChunkBatchSize)

	for split > 0 {
		groups = append(groups, firstChunksPerSeries[:split])
		firstChunksPerSeries = firstChunksPerSeries[split:]
		if len(firstChunksPerSeries) < split {
			split = len(firstChunksPerSeries)
		}
	}

	for _, group := range groups {
		err = fetchLazyChunks(ctx, group)
		if err != nil {
			return nil, err
		}

	outer:
		for _, chk := range group {
			for _, matcher := range matchers {
				if !matcher.Matches(chk.Chunk.Metric.Get(matcher.Name)) {
					continue outer
				}
			}

			m := chk.Chunk.Metric.Map()
			delete(m, labels.MetricName)
			results = append(results, logproto.SeriesIdentifier{
				Labels: m,
			})
		}
	}
	sort.Sort(results)
	return results, nil

}

// SelectLogs returns an iterator that will query the store for more chunks while iterating instead of fetching all chunks upfront
// for that request.
func (s *store) SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error) {
	matchers, from, through, err := decodeReq(req)
	if err != nil {
		return nil, err
	}

	lazyChunks, err := s.lazyChunks(ctx, matchers, from, through)
	if err != nil {
		return nil, err
	}

	expr, err := req.LogSelector()
	if err != nil {
		return nil, err
	}

	pipeline, err := expr.Pipeline()
	if err != nil {
		return nil, err
	}

	if len(lazyChunks) == 0 {
		return iter.NoopIterator, nil
	}

	return newLogBatchIterator(ctx, s.chunkMetrics, lazyChunks, s.cfg.MaxChunkBatchSize, matchers, pipeline, req.Direction, req.Start, req.End)

}

func (s *store) SelectSamples(ctx context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	matchers, from, through, err := decodeReq(req)
	if err != nil {
		return nil, err
	}

	expr, err := req.Expr()
	if err != nil {
		return nil, err
	}

	extractor, err := expr.Extractor()
	if err != nil {
		return nil, err
	}

	lazyChunks, err := s.lazyChunks(ctx, matchers, from, through)
	if err != nil {
		return nil, err
	}

	if len(lazyChunks) == 0 {
		return iter.NoopIterator, nil
	}
	return newSampleBatchIterator(ctx, s.chunkMetrics, lazyChunks, s.cfg.MaxChunkBatchSize, matchers, extractor, req.Start, req.End)
}

func (s *store) GetSchemaConfigs() []chunk.PeriodConfig {
	return s.schemaCfg.Configs
}

func filterChunksByTime(from, through model.Time, chunks []chunk.Chunk) []chunk.Chunk {
	filtered := make([]chunk.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.Through < from || through < chunk.From {
			continue
		}
		filtered = append(filtered, chunk)
	}
	return filtered
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

// UsingBoltdbShipper checks whether current or the next index type is boltdb-shipper, returns true if yes.
func UsingBoltdbShipper(configs []chunk.PeriodConfig) bool {
	activePCIndex := ActivePeriodConfig(configs)
	if configs[activePCIndex].IndexType == shipper.BoltDBShipperType ||
		(len(configs)-1 > activePCIndex && configs[activePCIndex+1].IndexType == shipper.BoltDBShipperType) {
		return true
	}

	return false
}
