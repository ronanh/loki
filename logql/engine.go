package logql

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promql_parser "github.com/prometheus/prometheus/promql/parser"
	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/stats"
)

var (
	queryTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logql",
		Name:      "query_duration_seconds",
		Help:      "LogQL query timings",
		Buckets:   prometheus.DefBuckets,
	}, []string{"query_type"})
	lastEntryMinTime = time.Unix(-100, 0)
)

// ValueTypeStreams promql.ValueType for log streams
const ValueTypeStreams = "streams"

// Streams is promql.Value
type Streams []logproto.Stream

func (streams Streams) Len() int      { return len(streams) }
func (streams Streams) Swap(i, j int) { streams[i], streams[j] = streams[j], streams[i] }
func (streams Streams) Less(i, j int) bool {
	return streams[i].Labels <= streams[j].Labels
}

// Type implements `promql.Value`
func (Streams) Type() promql_parser.ValueType { return ValueTypeStreams }

// String implements `promql.Value`
func (Streams) String() string {
	return ""
}

func (streams Streams) lines() int64 {
	var res int64
	for _, s := range streams {
		res += int64(len(s.Entries))
	}
	return res
}

// Result is the result of a query execution.
type Result struct {
	Data       promql_parser.Value
	Statistics stats.Result
}

// EngineOpts is the list of options to use with the LogQL query engine.
type EngineOpts struct {
	// Timeout for queries execution
	Timeout time.Duration `yaml:"timeout"`
	// MaxLookBackPeriod is the maximum amount of time to look back for log lines.
	// only used for instant log queries.
	MaxLookBackPeriod time.Duration `yaml:"max_look_back_period"`
	RecordMetrics     bool          `yaml:"record_metrics"`
	LogStats          bool          `yaml:"log_stats"`
}

func (opts *EngineOpts) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(
		&opts.Timeout,
		prefix+".engine.timeout",
		5*time.Minute,
		"Timeout for query execution.",
	)
	f.DurationVar(
		&opts.MaxLookBackPeriod,
		prefix+".engine.max-lookback-period",
		30*time.Second,
		"The maximum amount of time to look back for log lines. Used only for instant log queries.",
	)
	f.BoolVar(
		&opts.RecordMetrics,
		prefix+".engine.record-metrics",
		true,
		"Record metrics for queries.",
	)
	f.BoolVar(&opts.LogStats, prefix+".engine.log-stats", true, "Log query statistics.")
}

func (opts *EngineOpts) applyDefault() {
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Minute
	}
	if opts.MaxLookBackPeriod == 0 {
		opts.MaxLookBackPeriod = 30 * time.Second
	}
}

// Engine is the LogQL engine.
type Engine struct {
	timeout   time.Duration
	evaluator Evaluator
	opts      EngineOpts
}

// NewEngine creates a new LogQL Engine.
func NewEngine(opts EngineOpts, q Querier) *Engine {
	opts.applyDefault()
	return &Engine{
		timeout:   opts.Timeout,
		evaluator: NewDefaultEvaluator(q, opts.MaxLookBackPeriod),
		opts:      opts,
	}
}

// Query creates a new LogQL query. Instant/Range type is derived from the parameters.
func (ng *Engine) Query(params Params) Query {
	return &query{
		timeout:   ng.timeout,
		params:    params,
		evaluator: ng.evaluator,
		parse: func(_ context.Context, query string) (Expr, error) {
			return ParseExpr(query)
		},
		record:   ng.opts.RecordMetrics,
		logStats: ng.opts.LogStats,
	}
}

// Query is a LogQL query to be executed.
type Query interface {
	// Exec processes the query.
	Exec(ctx context.Context) (Result, error)
}

type query struct {
	timeout   time.Duration
	params    Params
	parse     func(context.Context, string) (Expr, error)
	evaluator Evaluator
	record    bool
	logStats  bool
	hashBuf   []byte
}

// Exec Implements `Query`. It handles instrumentation & defers to Eval.
func (q *query) Exec(ctx context.Context) (Result, error) {
	rangeType := GetRangeType(q.params)
	timer := prometheus.NewTimer(queryTime.WithLabelValues(string(rangeType)))
	defer timer.ObserveDuration()

	// records query statistics
	var statResult stats.Result
	start := time.Now()
	if q.logStats || q.record {
		ctx = stats.NewContext(ctx)
	}
	data, err := q.Eval(ctx)

	if q.logStats || q.record {
		statResult = stats.Snapshot(ctx, time.Since(start))
	}
	if q.logStats {
		statResult.Log(ctx, slog.LevelDebug, slog.Default())
	}

	status := "200"
	if err != nil {
		status = "500"
		if errors.Is(err, ErrParse) ||
			errors.Is(err, ErrPipeline) ||
			errors.Is(err, ErrLimit) ||
			errors.Is(err, context.Canceled) {
			status = "400"
		}
	}

	if q.record {
		RecordMetrics(ctx, q.params, status, statResult, data)
	}

	return Result{
		Data:       data,
		Statistics: statResult,
	}, err
}

func (q *query) Eval(ctx context.Context) (promql_parser.Value, error) {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	expr, err := q.parse(ctx, q.params.Query())
	if err != nil {
		return nil, err
	}

	switch e := expr.(type) {
	case SampleExpr:
		value, err := q.evalSample(ctx, e)
		return value, err

	case LogSelectorExpr:
		iter, err := q.evaluator.Iterator(ctx, e, q.params)
		if err != nil {
			return nil, err
		}

		defer func() {
			if err := iter.Close(); err != nil {
				slog.ErrorContext(ctx, "closing iterator", "err", err)
			}
		}()
		streams, err := readStreams(iter, q.params.Limit(), q.params.Direction(), q.params.Interval())
		return streams, err
	default:
		return nil, errors.New("unexpected type (%T): cannot evaluate")
	}
}

// evalSample evaluate a sampleExpr
func (q *query) evalSample(ctx context.Context, expr SampleExpr) (promql_parser.Value, error) {
	if lit, ok := expr.(*literalExpr); ok {
		return q.evalLiteral(ctx, lit)
	}

	expr, err := optimizeSampleExpr(expr)
	if err != nil {
		return nil, err
	}

	stepEvaluator, err := q.evaluator.StepEvaluator(ctx, q.evaluator, expr, q.params)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := stepEvaluator.Close(); err != nil {
			slog.ErrorContext(ctx, "closing SampleExpr", "err", err)
		}
	}()

	seriesIndex := map[uint64]*promql.Series{}

	next, ts, vec := stepEvaluator.Next()
	if stepEvaluator.Error() != nil {
		return nil, stepEvaluator.Error()
	}

	if GetRangeType(q.params) == InstantType {
		sort.Slice(
			vec,
			func(i, j int) bool { return labels.Compare(vec[i].Metric, vec[j].Metric) < 0 },
		)
		return vec, nil
	}

	stepCount := int(
		math.Ceil(
			float64(
				q.params.End().Sub(q.params.Start()).Nanoseconds(),
			) / float64(
				q.params.Step().Nanoseconds(),
			),
		),
	)
	if stepCount <= 0 {
		stepCount = 1
	}

	for next {
		for _, p := range vec {
			var (
				series *promql.Series
				hash   uint64
				ok     bool
			)
			hash, q.hashBuf = HashLabels(q.hashBuf, p.Metric)

			series, ok = seriesIndex[hash]
			if !ok {
				series = &promql.Series{
					Metric: p.Metric,
					Points: make([]promql.Point, 0, stepCount),
				}
				seriesIndex[hash] = series
			}
			series.Points = append(series.Points, promql.Point{
				T: ts,
				V: p.V,
			})
		}
		next, ts, vec = stepEvaluator.Next()
		if stepEvaluator.Error() != nil {
			return nil, stepEvaluator.Error()
		}
	}

	series := make([]promql.Series, 0, len(seriesIndex))
	for _, s := range seriesIndex {
		series = append(series, *s)
	}
	result := promql.Matrix(series)
	sort.Sort(result)

	return result, stepEvaluator.Error()
}

func (q *query) evalLiteral(_ context.Context, expr *literalExpr) (promql_parser.Value, error) {
	s := promql.Scalar{
		T: q.params.Start().UnixNano() / int64(time.Millisecond),
		V: expr.value,
	}

	if GetRangeType(q.params) == InstantType {
		return s, nil
	}

	return PopulateMatrixFromScalar(s, q.params), nil
}

func PopulateMatrixFromScalar(data promql.Scalar, params Params) promql.Matrix {
	var (
		start  = params.Start()
		end    = params.End()
		step   = params.Step()
		series = promql.Series{
			Points: make(
				[]promql.Point,
				0,
				// allocate enough space for all needed entries
				int(end.Sub(start)/step)+1,
			),
		}
	)

	for ts := start; !ts.After(end); ts = ts.Add(step) {
		series.Points = append(series.Points, promql.Point{
			T: ts.UnixNano() / int64(time.Millisecond),
			V: data.V,
		})
	}
	return promql.Matrix{series}
}

func readStreams(
	i iter.EntryIterator,
	size uint32,
	dir logproto.Direction,
	interval time.Duration,
) (Streams, error) {
	streams := map[string]*logproto.Stream{}
	respSize := uint32(0)
	// lastEntry should be a really old time so that the first comparison is always true, we use a
	// negative
	// value here because many unit tests start at time.Unix(0,0)
	lastEntry := lastEntryMinTime
	for respSize < size && i.Next() {
		labels, entry := i.Labels(), i.Entry()
		forwardShouldOutput := dir == logproto.FORWARD &&
			(i.Entry().Timestamp.Equal(lastEntry.Add(interval)) || i.Entry().Timestamp.After(lastEntry.Add(interval)))
		backwardShouldOutput := dir == logproto.BACKWARD &&
			(i.Entry().Timestamp.Equal(lastEntry.Add(-interval)) || i.Entry().Timestamp.Before(lastEntry.Add(-interval)))
		// If step == 0 output every line.
		// If lastEntry.Unix < 0 this is the first pass through the loop and we should output the
		// line.
		// Then check to see if the entry is equal to, or past a forward or reverse step
		if interval == 0 || lastEntry.Unix() < 0 || forwardShouldOutput || backwardShouldOutput {
			stream, ok := streams[labels]
			if !ok {
				stream = &logproto.Stream{
					Labels: labels,
				}
				streams[labels] = stream
			}
			stream.Entries = append(stream.Entries, entry)
			lastEntry = i.Entry().Timestamp
			respSize++
		}
	}

	result := make(Streams, 0, len(streams))
	for _, stream := range streams {
		result = append(result, *stream)
	}
	sort.Sort(result)
	return result, i.Error()
}
