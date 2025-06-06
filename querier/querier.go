package querier

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/health/grpc_health_v1"

	cortex_validation "github.com/cortexproject/cortex/pkg/util/validation"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/loghttp"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
	"github.com/ronanh/loki/storage"
	listutil "github.com/ronanh/loki/util"
	"github.com/ronanh/loki/util/validation"
)

const (
	// How long the Tailer should wait - once there are no entries to read from ingesters -
	// before checking if a new entry is available (to avoid spinning the CPU in a continuous
	// check loop)
	tailerWaitEntryThrottle = time.Second / 2
)

type interval struct {
	start, end time.Time
}

// Config for a querier.
type Config struct {
	QueryTimeout                  time.Duration    `yaml:"query_timeout"`
	TailMaxDuration               time.Duration    `yaml:"tail_max_duration"`
	ExtraQueryDelay               time.Duration    `yaml:"extra_query_delay,omitempty"`
	QueryIngestersWithin          time.Duration    `yaml:"query_ingesters_within,omitempty"`
	IngesterQueryStoreMaxLookback time.Duration    `yaml:"-"`
	Engine                        logql.EngineOpts `yaml:"engine,omitempty"`
	MaxConcurrent                 int              `yaml:"max_concurrent"`
}

// RegisterFlags register flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Engine.RegisterFlagsWithPrefix("querier", f)
	f.DurationVar(&cfg.TailMaxDuration, "querier.tail-max-duration", 1*time.Hour, "Limit the duration for which live tailing request would be served")
	f.DurationVar(&cfg.QueryTimeout, "querier.query-timeout", 1*time.Minute, "Timeout when querying backends (ingesters or storage) during the execution of a query request")
	f.DurationVar(&cfg.ExtraQueryDelay, "querier.extra-query-delay", 0, "Time to wait before sending more than the minimum successful query requests.")
	f.DurationVar(&cfg.QueryIngestersWithin, "querier.query-ingesters-within", 0, "Maximum lookback beyond which queries are not sent to ingester. 0 means all queries are sent to ingester.")
	f.IntVar(&cfg.MaxConcurrent, "querier.max-concurrent", 20, "The maximum number of concurrent queries.")
}

// Querier handlers queries.
type Querier interface {
	SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error)
	SelectSamples(ctx context.Context, params logql.SelectSampleParams) (iter.SampleIterator, error)
	Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error)
	Tail(ctx context.Context, req *logproto.TailRequest) (*Tailer, error)
	Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error)
}

// querier handlers queries.
type querier struct {
	cfg             Config
	store           storage.Store
	limits          *validation.Overrides
	ingesterQuerier *IngesterQuerier
}

// New makes a new querier.
func New(cfg Config, store storage.Store, ingesterQuerier *IngesterQuerier, limits *validation.Overrides) (*querier, error) {
	qr := querier{
		cfg:             cfg,
		store:           store,
		ingesterQuerier: ingesterQuerier,
		limits:          limits,
	}

	return &qr, nil
}

// Select Implements logql.Querier which select logs via matchers and regex filters.
func (q *querier) SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error) {
	err := q.validateQueryRequest(ctx, params)
	if err != nil {
		return nil, err
	}

	ingesterQueryInterval, storeQueryInterval := q.buildQueryIntervals(params.Start, params.End)

	iters := []iter.EntryIterator{}
	if ingesterQueryInterval != nil {
		// Make a copy of the request before modifying
		// because the initial request is used below to query stores
		queryRequestCopy := *params.QueryRequest
		newParams := logql.SelectLogParams{
			QueryRequest: &queryRequestCopy,
		}
		newParams.Start = ingesterQueryInterval.start
		newParams.End = ingesterQueryInterval.end

		ingesterIters, err := q.ingesterQuerier.SelectLogs(ctx, newParams)
		if err != nil {
			return nil, err
		}

		iters = append(iters, ingesterIters...)
	}

	if storeQueryInterval != nil {
		params.Start = storeQueryInterval.start
		params.End = storeQueryInterval.end

		storeIter, err := q.store.SelectLogs(ctx, params)
		if err != nil {
			return nil, err
		}

		iters = append(iters, storeIter)
	}

	return iter.NewHeapIterator(ctx, iters, params.Direction), nil
}

func (q *querier) SelectSamples(ctx context.Context, params logql.SelectSampleParams) (iter.SampleIterator, error) {
	err := q.validateQueryRequest(ctx, params)
	if err != nil {
		return nil, err
	}

	ingesterQueryInterval, storeQueryInterval := q.buildQueryIntervals(params.Start, params.End)

	iters := []iter.SampleIterator{}
	if ingesterQueryInterval != nil {
		// Make a copy of the request before modifying
		// because the initial request is used below to query stores
		queryRequestCopy := *params.SampleQueryRequest
		newParams := logql.SelectSampleParams{
			SampleQueryRequest: &queryRequestCopy,
		}
		newParams.Start = ingesterQueryInterval.start
		newParams.End = ingesterQueryInterval.end

		ingesterIters, err := q.ingesterQuerier.SelectSample(ctx, newParams)
		if err != nil {
			return nil, err
		}

		iters = append(iters, ingesterIters...)
	}

	if storeQueryInterval != nil {
		params.Start = storeQueryInterval.start
		params.End = storeQueryInterval.end

		storeIter, err := q.store.SelectSamples(ctx, params)
		if err != nil {
			return nil, err
		}

		iters = append(iters, storeIter)
	}
	return iter.NewHeapSampleIterator(ctx, iters), nil
}

func (q *querier) buildQueryIntervals(queryStart, queryEnd time.Time) (*interval, *interval) {
	// limitQueryInterval is a flag for whether store queries should be limited to start time of ingester queries.
	limitQueryInterval := false
	// ingesterMLB having -1 means query ingester for whole duration.
	ingesterMLB := time.Duration(-1)
	if q.cfg.IngesterQueryStoreMaxLookback != 0 {
		// IngesterQueryStoreMaxLookback takes the precedence over QueryIngestersWithin while also limiting the store query range.
		limitQueryInterval = true
		ingesterMLB = q.cfg.IngesterQueryStoreMaxLookback
	} else if q.cfg.QueryIngestersWithin != 0 {
		ingesterMLB = q.cfg.QueryIngestersWithin
	}

	// query ingester for whole duration.
	if ingesterMLB == -1 {
		i := &interval{
			start: queryStart,
			end:   queryEnd,
		}

		if limitQueryInterval {
			// query only ingesters.
			return i, nil
		}

		// query both stores and ingesters without limiting the query interval.
		return i, i
	}

	// see if there is an overlap between ingester query interval and actual query interval, if not just do the store query.
	ingesterOldestStartTime := time.Now().Add(-ingesterMLB)
	if queryEnd.Before(ingesterOldestStartTime) {
		return nil, &interval{
			start: queryStart,
			end:   queryEnd,
		}
	}

	// if there is an overlap and we are not limiting the query interval then do both store and ingester query for whole query interval.
	if !limitQueryInterval {
		i := &interval{
			start: queryStart,
			end:   queryEnd,
		}
		return i, i
	}

	// since we are limiting the query interval, check if the query touches just the ingesters, if yes then query just the ingesters.
	if ingesterOldestStartTime.Before(queryStart) {
		return &interval{
			start: queryStart,
			end:   queryEnd,
		}, nil
	}

	// limit the start of ingester query interval to ingesterOldestStartTime.
	ingesterQueryInterval := &interval{
		start: ingesterOldestStartTime,
		end:   queryEnd,
	}

	// limit the end of ingester query interval to ingesterOldestStartTime.
	storeQueryInterval := &interval{
		start: queryStart,
		end:   ingesterOldestStartTime,
	}

	// query touches only ingester query interval so do not do store query.
	if storeQueryInterval.start.After(storeQueryInterval.end) {
		storeQueryInterval = nil
	}

	return ingesterQueryInterval, storeQueryInterval
}

// Label does the heavy lifting for a Label query.
func (q *querier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	if err = q.validateQueryTimeRange(userID, *req.Start, *req.End); err != nil {
		return nil, err
	}

	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	ingesterValues, err := q.ingesterQuerier.Label(ctx, req)
	if err != nil {
		return nil, err
	}

	var matchers []*labels.Matcher
	if req.Query != "" {
		matchers, err = logql.ParseMatchers(req.Query)
		if err != nil {
			return nil, err
		}
	}

	from, through := model.TimeFromUnixNano(req.Start.UnixNano()), model.TimeFromUnixNano(req.End.UnixNano())
	var storeValues []string
	if req.Values {
		storeValues, err = q.store.LabelValuesForMetricName(ctx, userID, from, through, "logs", req.Name, matchers...)
		if err != nil {
			return nil, err
		}
	} else {
		storeValues, err = q.store.LabelNamesForMetricName(ctx, userID, from, through, "logs", matchers...)
		if err != nil {
			return nil, err
		}
	}

	results := append(ingesterValues, storeValues)

	return &logproto.LabelResponse{
		Values: listutil.MergeStringLists(results...),
	}, nil
}

// Check implements the grpc healthcheck
func (*querier) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Tail keeps getting matching logs from all ingesters for given query
func (q *querier) Tail(ctx context.Context, req *logproto.TailRequest) (*Tailer, error) {
	err := q.checkTailRequestLimit(ctx)
	if err != nil {
		return nil, err
	}

	histReq := logql.SelectLogParams{
		QueryRequest: &logproto.QueryRequest{
			Selector:  req.Query,
			Start:     req.Start,
			End:       time.Now(),
			Limit:     req.Limit,
			Direction: logproto.BACKWARD,
		},
	}

	err = q.validateQueryRequest(ctx, histReq)
	if err != nil {
		return nil, err
	}

	// Enforce the query timeout except when tailing, otherwise the tailing
	// will be terminated once the query timeout is reached
	tailCtx := ctx
	queryCtx, cancelQuery := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancelQuery()

	tailClients, err := q.ingesterQuerier.Tail(tailCtx, req)
	if err != nil {
		return nil, err
	}

	histIterators, err := q.SelectLogs(queryCtx, histReq)
	if err != nil {
		return nil, err
	}

	reversedIterator, err := iter.NewReversedIter(histIterators, req.Limit, true)
	if err != nil {
		return nil, err
	}

	return newTailer(
		time.Duration(req.DelayFor)*time.Second,
		tailClients,
		reversedIterator,
		func(connectedIngestersAddr []string) (map[string]logproto.Querier_TailClient, error) {
			return q.ingesterQuerier.TailDisconnectedIngesters(tailCtx, req, connectedIngestersAddr)
		},
		q.cfg.TailMaxDuration,
		tailerWaitEntryThrottle,
	), nil
}

// Series fetches any matching series for a list of matcher sets
func (q *querier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	if err = q.validateQueryTimeRange(userID, req.Start, req.End); err != nil {
		return nil, err
	}

	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	return q.awaitSeries(ctx, req)

}

func (q *querier) awaitSeries(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {

	// buffer the channels to the # of calls they're expecting su
	series := make(chan [][]logproto.SeriesIdentifier, 2)
	errs := make(chan error, 2)

	// fetch series from ingesters and store concurrently

	go func() {
		// fetch series identifiers from ingesters
		resps, err := q.ingesterQuerier.Series(ctx, req)
		if err != nil {
			errs <- err
			return
		}

		series <- resps
	}()

	go func() {
		storeValues, err := q.seriesForMatchers(ctx, req.Start, req.End, req.GetGroups())
		if err != nil {
			errs <- err
			return
		}
		series <- [][]logproto.SeriesIdentifier{storeValues}
	}()

	var sets [][]logproto.SeriesIdentifier
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			return nil, err
		case s := <-series:
			sets = append(sets, s...)
		}
	}

	deduped := make(map[string]logproto.SeriesIdentifier)
	for _, set := range sets {
		for _, s := range set {
			key := loghttp.LabelSet(s.Labels).String()
			if _, exists := deduped[key]; !exists {
				deduped[key] = s
			}
		}
	}

	response := &logproto.SeriesResponse{
		Series: make([]logproto.SeriesIdentifier, 0, len(deduped)),
	}

	for _, s := range deduped {
		response.Series = append(response.Series, s)
	}

	return response, nil
}

// seriesForMatchers fetches series from the store for each matcher set
// TODO: make efficient if/when the index supports labels so we don't have to read chunks
func (q *querier) seriesForMatchers(
	ctx context.Context,
	from, through time.Time,
	groups []string,
) ([]logproto.SeriesIdentifier, error) {

	var results []logproto.SeriesIdentifier
	// If no matchers were specified for the series query,
	// we send a query with an empty matcher which will match every series.
	if len(groups) == 0 {
		var err error
		results, err = q.seriesForMatcher(ctx, from, through, "")
		if err != nil {
			return nil, err
		}
	} else {
		for _, group := range groups {
			ids, err := q.seriesForMatcher(ctx, from, through, group)
			if err != nil {
				return nil, err
			}
			results = append(results, ids...)
		}
	}
	return results, nil
}

// seriesForMatcher fetches series from the store for a given matcher
func (q *querier) seriesForMatcher(ctx context.Context, from, through time.Time, matcher string) ([]logproto.SeriesIdentifier, error) {
	ids, err := q.store.GetSeries(ctx, logql.SelectLogParams{
		QueryRequest: &logproto.QueryRequest{
			Selector:  matcher,
			Limit:     1,
			Start:     from,
			End:       through,
			Direction: logproto.FORWARD,
		},
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (q *querier) validateQueryRequest(ctx context.Context, req logql.QueryParams) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	selector, err := req.LogSelector()
	if err != nil {
		return err
	}
	matchers := selector.Matchers()

	maxStreamMatchersPerQuery := q.limits.MaxStreamsMatchersPerQuery(userID)
	if len(matchers) > maxStreamMatchersPerQuery {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max streams matchers per query exceeded, matchers-count > limit (%d > %d)", len(matchers), maxStreamMatchersPerQuery)
	}

	return q.validateQueryTimeRange(userID, req.GetStart(), req.GetEnd())
}

func (q *querier) validateQueryTimeRange(userID string, from time.Time, through time.Time) error {
	if (through).Before(from) {
		return httpgrpc.Errorf(http.StatusBadRequest, "invalid query, through < from (%s < %s)", through, from)
	}

	maxQueryLength := q.limits.MaxQueryLength(userID)
	if maxQueryLength > 0 && (through).Sub(from) > maxQueryLength {
		return httpgrpc.Errorf(http.StatusBadRequest, cortex_validation.ErrQueryTooLong, (through).Sub(from), maxQueryLength)
	}

	return nil
}

func (q *querier) checkTailRequestLimit(ctx context.Context) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	responses, err := q.ingesterQuerier.TailersCount(ctx)
	// We are only checking active ingesters, and any error returned stops checking other ingesters
	// so return that error here as well.
	if err != nil {
		return err
	}

	var maxCnt uint32
	maxCnt = 0
	for _, resp := range responses {
		if resp > maxCnt {
			maxCnt = resp
		}
	}
	l := uint32(q.limits.MaxConcurrentTailRequests(userID))
	if maxCnt >= l {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max concurrent tail requests limit exceeded, count > limit (%d > %d)", maxCnt+1, l)
	}

	return nil
}
