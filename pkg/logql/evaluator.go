package logql

import (
	"container/heap"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/ronanh/loki/pkg/iter"
	"github.com/ronanh/loki/pkg/logproto"
	"github.com/ronanh/loki/pkg/logql/log"
	"github.com/ronanh/loki/pkg/util"
)

type QueryRangeType string

var (
	InstantType QueryRangeType = "instant"
	RangeType   QueryRangeType = "range"
)

// Params details the parameters associated with a loki request
type Params interface {
	Query() string
	Start() time.Time
	End() time.Time
	Step() time.Duration
	Interval() time.Duration
	Limit() uint32
	Direction() logproto.Direction
	Shards() []string
}

func NewLiteralParams(
	qs string,
	start, end time.Time,
	step, interval time.Duration,
	direction logproto.Direction,
	limit uint32,
	shards []string,
) LiteralParams {
	return LiteralParams{
		qs:        qs,
		start:     start,
		end:       end,
		step:      step,
		interval:  interval,
		direction: direction,
		limit:     limit,
		shards:    shards,
	}
}

// LiteralParams impls Params
type LiteralParams struct {
	qs         string
	start, end time.Time
	step       time.Duration
	interval   time.Duration
	direction  logproto.Direction
	limit      uint32
	shards     []string
}

func (p LiteralParams) Copy() LiteralParams { return p }

// String impls Params
func (p LiteralParams) Query() string { return p.qs }

// Start impls Params
func (p LiteralParams) Start() time.Time { return p.start }

// End impls Params
func (p LiteralParams) End() time.Time { return p.end }

// Step impls Params
func (p LiteralParams) Step() time.Duration { return p.step }

// Interval impls Params
func (p LiteralParams) Interval() time.Duration { return p.interval }

// Limit impls Params
func (p LiteralParams) Limit() uint32 { return p.limit }

// Direction impls Params
func (p LiteralParams) Direction() logproto.Direction { return p.direction }

// Shards impls Params
func (p LiteralParams) Shards() []string { return p.shards }

// GetRangeType returns whether a query is an instant query or range query
func GetRangeType(q Params) QueryRangeType {
	if q.Start() == q.End() && q.Step() == 0 {
		return InstantType
	}
	return RangeType
}

// Evaluator is an interface for iterating over data at different nodes in the AST
type Evaluator interface {
	SampleEvaluator
	EntryEvaluator
}

type SampleEvaluator interface {
	// StepEvaluator returns a StepEvaluator for a given SampleExpr. It's explicitly passed another StepEvaluator// in order to enable arbitrary computation of embedded expressions. This allows more modular & extensible
	// StepEvaluator implementations which can be composed.
	StepEvaluator(ctx context.Context, nextEvaluator SampleEvaluator, expr SampleExpr, p Params) (StepEvaluator, error)
}

type SampleEvaluatorFunc func(ctx context.Context, nextEvaluator SampleEvaluator, expr SampleExpr, p Params) (StepEvaluator, error)

func (s SampleEvaluatorFunc) StepEvaluator(ctx context.Context, nextEvaluator SampleEvaluator, expr SampleExpr, p Params) (StepEvaluator, error) {
	return s(ctx, nextEvaluator, expr, p)
}

type EntryEvaluator interface {
	// Iterator returns the iter.EntryIterator for a given LogSelectorExpr
	Iterator(context.Context, LogSelectorExpr, Params) (iter.EntryIterator, error)
}

// EvaluatorUnsupportedType is a helper for signaling that an evaluator does not support an Expr type
func EvaluatorUnsupportedType(expr Expr, ev Evaluator) error {
	return errors.Errorf("unexpected expr type (%T) for Evaluator type (%T) ", expr, ev)
}

type DefaultEvaluator struct {
	maxLookBackPeriod time.Duration
	querier           Querier
}

// NewDefaultEvaluator constructs a DefaultEvaluator
func NewDefaultEvaluator(querier Querier, maxLookBackPeriod time.Duration) *DefaultEvaluator {
	return &DefaultEvaluator{
		querier:           querier,
		maxLookBackPeriod: maxLookBackPeriod,
	}
}

func (ev *DefaultEvaluator) Iterator(ctx context.Context, expr LogSelectorExpr, q Params) (iter.EntryIterator, error) {
	params := SelectLogParams{
		QueryRequest: &logproto.QueryRequest{
			Start:     q.Start(),
			End:       q.End(),
			Limit:     q.Limit(),
			Direction: q.Direction(),
			Selector:  expr.String(),
			Shards:    q.Shards(),
		},
	}

	if GetRangeType(q) == InstantType {
		params.Start = params.Start.Add(-ev.maxLookBackPeriod)
	}

	return ev.querier.SelectLogs(ctx, params)
}

func (ev *DefaultEvaluator) StepEvaluator(
	ctx context.Context,
	nextEv SampleEvaluator,
	expr SampleExpr,
	q Params,
) (StepEvaluator, error) {
	switch e := expr.(type) {
	case *vectorAggregationExpr:
		if rangExpr, ok := e.left.(*rangeAggregationExpr); ok && e.operation == OpTypeSum {
			// if range expression is wrapped with a vector expression
			// we should send the vector expression for allowing reducing labels at the source.
			nextEv = SampleEvaluatorFunc(func(ctx context.Context, nextEvaluator SampleEvaluator, expr SampleExpr, p Params) (StepEvaluator, error) {
				it, err := ev.querier.SelectSamples(ctx, SelectSampleParams{
					&logproto.SampleQueryRequest{
						Start:    q.Start().Add(-rangExpr.left.interval),
						End:      q.End(),
						Selector: e.String(), // intentionally send the the vector for reducing labels.
						Shards:   q.Shards(),
					},
				})
				if err != nil {
					return nil, err
				}
				return rangeAggEvaluator(iter.NewPeekingSampleIterator(it), rangExpr, q)
			})
		}
		return vectorAggEvaluator(ctx, nextEv, e, q)
	case *rangeAggregationExpr:
		it, err := ev.querier.SelectSamples(ctx, SelectSampleParams{
			&logproto.SampleQueryRequest{
				Start:    q.Start().Add(-e.left.interval),
				End:      q.End(),
				Selector: expr.String(),
				Shards:   q.Shards(),
			},
		})
		if err != nil {
			return nil, err
		}
		return rangeAggEvaluator(iter.NewPeekingSampleIterator(it), e, q)
	case *binOpExpr:
		return newBinOpStepEvaluator(ctx, nextEv, e, q)
	case *labelReplaceExpr:
		return labelReplaceEvaluator(ctx, nextEv, e, q)
	default:
		return nil, EvaluatorUnsupportedType(e, ev)
	}
}

var (
	evaluatorGroupsPool = sync.Pool{
		New: func() interface{} {
			return newEvaluatorGroups()
		},
	}
)

const (
	maxUnsortedGroups = 8
)

type evaluatorGroups struct {
	groups []metricGroup
	sorted bool
	// temporary variables
	stepResults []stepResult
	labelsAlloc labels.Labels
	hashBuf     []byte
}

func newEvaluatorGroups() *evaluatorGroups {
	return &evaluatorGroups{
		groups:  make([]metricGroup, 0, 8),
		hashBuf: make([]byte, 0, 1024),
	}
}

type metricGroup struct {
	hash       uint64
	labels     labels.Labels
	stepResult int
}

type stepResult struct {
	labels      labels.Labels
	value       float64
	mean        float64
	groupCount  int
	heap        vectorByValueHeap
	reverseHeap vectorByReverseValueHeap
}

func (eg *evaluatorGroups) searchLabels(hash uint64) int {
	// find the value node
	if eg.sorted {
		// implement custom binary search algorithm to find the value node
		// as the values are sorted by hash
		i, j := 0, len(eg.groups)
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			if eg.groups[h].hash < hash {
				i = h + 1
			} else {
				j = h
			}
		}
		return i
	}
	// find the value node using linear search
	for i := range eg.groups {
		if eg.groups[i].hash == hash {
			return i
		}
	}
	return len(eg.groups)
}

func (eg *evaluatorGroups) getLabelsGroup(lbls labels.Labels, groups []string, without bool) *metricGroup {
	// switch to sorted mode if the number of groups is greater than maxUnsortedLabels
	if !eg.sorted && len(eg.groups) > maxUnsortedGroups {
		eg.sorted = true
		sort.Slice(eg.groups, func(i, j int) bool {
			return eg.groups[i].hash < eg.groups[j].hash
		})
	}

	// hash the grouped labels
	var hash uint64
	if without {
		hash, eg.hashBuf = HashWithoutLabels(eg.hashBuf, lbls, groups...)
	} else {
		hash, eg.hashBuf = HashForLabels(eg.hashBuf, lbls, groups...)
	}

	// find the group by hash
	i := eg.searchLabels(hash)
	if i < len(eg.groups) && eg.groups[i].hash == hash {
		return &eg.groups[i]
	}

	// group not found, create a new group

	// build the group labels
	if len(lbls) > cap(eg.labelsAlloc) || cap(eg.labelsAlloc) == 0 {
		eg.labelsAlloc = make(labels.Labels, 0, 1024)
	}
	if without {
		var iStartGroup int
		for _, lbl := range lbls {
			var found bool
			for i := iStartGroup; i < len(groups); i++ {
				if lbl.Name == groups[i] {
					found = true
					iStartGroup = i + 1
					break
				}
			}
			if !found {
				eg.labelsAlloc = append(eg.labelsAlloc, lbl)
			}
		}
	} else {
		var iStartLabels int
		for _, g := range groups {
			for i := iStartLabels; i < len(lbls); i++ {
				if lbls[i].Name == g {
					eg.labelsAlloc = append(eg.labelsAlloc, lbls[i])
					iStartLabels = i + 1
					break
				}
			}
		}
	}
	lbls = eg.labelsAlloc[:len(eg.labelsAlloc):len(eg.labelsAlloc)]
	eg.labelsAlloc = eg.labelsAlloc[len(lbls):]

	// add the new group
	if eg.sorted {
		eg.groups = append(eg.groups, metricGroup{})
		copy(eg.groups[i+1:], eg.groups[i:])
		eg.groups[i] = metricGroup{hash: hash, labels: lbls, stepResult: -1}
	} else {
		i = len(eg.groups)
		eg.groups = append(eg.groups, metricGroup{hash: hash, labels: lbls, stepResult: -1})
	}
	return &eg.groups[i]
}

func vectorAggEvaluator(
	ctx context.Context,
	ev SampleEvaluator,
	expr *vectorAggregationExpr,
	q Params,
) (StepEvaluator, error) {
	nextEvaluator, err := ev.StepEvaluator(ctx, ev, expr.left, q)
	if err != nil {
		return nil, err
	}
	sort.Strings(expr.grouping.groups)
	eg := evaluatorGroupsPool.Get().(*evaluatorGroups)
	return newStepEvaluator(func() (bool, int64, promql.Vector) {
		next, ts, vec := nextEvaluator.Next()

		if !next {
			return false, 0, promql.Vector{}
		}
		eg.stepResults = eg.stepResults[:0]

		if expr.operation == OpTypeTopK || expr.operation == OpTypeBottomK {
			if expr.params < 1 {
				return next, ts, promql.Vector{}
			}
		}
		// reset step results
		for i := range eg.groups {
			eg.groups[i].stepResult = -1
		}
		for _, s := range vec {
			metric := s.Metric
			group := eg.getLabelsGroup(metric, expr.grouping.groups, expr.grouping.without)

			if group.stepResult == -1 {
				// new step result
				group.stepResult = len(eg.stepResults)
				eg.stepResults = append(eg.stepResults, stepResult{
					labels:     group.labels,
					value:      s.V,
					mean:       s.V,
					groupCount: 1,
				})
				result := &eg.stepResults[group.stepResult]

				resultSize := min(expr.params, len(vec))
				if expr.operation == OpTypeStdvar || expr.operation == OpTypeStddev {
					result.value = 0.0
				} else if expr.operation == OpTypeTopK {
					groupHeap := make(vectorByValueHeap, 0, resultSize)
					heap.Push(&groupHeap, &promql.Sample{
						Point:  promql.Point{V: s.V},
						Metric: s.Metric,
					})
					result.heap = groupHeap
				} else if expr.operation == OpTypeBottomK {
					groupReverseHeap := make(vectorByReverseValueHeap, 0, resultSize)
					heap.Push(&groupReverseHeap, &promql.Sample{
						Point:  promql.Point{V: s.V},
						Metric: s.Metric,
					})
					result.reverseHeap = groupReverseHeap
				}
			} else {
				result := &eg.stepResults[group.stepResult]
				// aggregate step result
				switch expr.operation {
				case OpTypeSum:
					result.value += s.V

				case OpTypeAvg:
					result.groupCount++
					result.mean += (s.V - result.mean) / float64(result.groupCount)

				case OpTypeMax:
					if result.value < s.V || math.IsNaN(result.value) {
						result.value = s.V
					}

				case OpTypeMin:
					if result.value > s.V || math.IsNaN(result.value) {
						result.value = s.V
					}

				case OpTypeCount:
					result.groupCount++

				case OpTypeStddev, OpTypeStdvar:
					result.groupCount++
					delta := s.V - result.mean
					result.mean += delta / float64(result.groupCount)
					result.value += delta * (s.V - result.mean)

				case OpTypeTopK:
					if len(result.heap) < expr.params || result.heap[0].V < s.V || math.IsNaN(result.heap[0].V) {
						groupHeap := result.heap
						if len(groupHeap) == expr.params {
							heap.Pop(&groupHeap)
						}
						heap.Push(&groupHeap, &promql.Sample{
							Point:  promql.Point{V: s.V},
							Metric: s.Metric,
						})
						result.heap = groupHeap
					}

				case OpTypeBottomK:
					if len(result.reverseHeap) < expr.params || result.reverseHeap[0].V > s.V || math.IsNaN(result.reverseHeap[0].V) {
						groupReverseHeap := result.reverseHeap
						if len(groupReverseHeap) == expr.params {
							heap.Pop(&groupReverseHeap)
						}
						heap.Push(&groupReverseHeap, &promql.Sample{
							Point:  promql.Point{V: s.V},
							Metric: s.Metric,
						})
						result.reverseHeap = groupReverseHeap
					}
				default:
					panic(errors.Errorf("expected aggregation operator but got %q", expr.operation))
				}
			}
		}
		vec = vec[:0]
		for i := range eg.stepResults {
			result := &eg.stepResults[i]
			// reset step result for next step
			switch expr.operation {
			case OpTypeAvg:
				result.value = result.mean
			case OpTypeCount:
				result.value = float64(result.groupCount)
			case OpTypeStddev:
				result.value = math.Sqrt(result.value / float64(result.groupCount))
			case OpTypeStdvar:
				result.value = result.value / float64(result.groupCount)
			case OpTypeTopK:
				// The heap keeps the lowest value on top, so reverse it.
				aggrHeap := result.heap
				sort.Sort(sort.Reverse(aggrHeap))
				for _, v := range aggrHeap {
					vec = append(vec, promql.Sample{
						Metric: v.Metric,
						Point: promql.Point{
							T: ts,
							V: v.V,
						},
					})
				}
				continue // Bypass default append.

			case OpTypeBottomK:
				// The heap keeps the lowest value on top, so reverse it.
				aggrReverseHeap := result.reverseHeap
				sort.Sort(sort.Reverse(aggrReverseHeap))
				for _, v := range aggrReverseHeap {
					vec = append(vec, promql.Sample{
						Metric: v.Metric,
						Point: promql.Point{
							T: ts,
							V: v.V,
						},
					})
				}
				continue // Bypass default append.
			}
			vec = append(vec, promql.Sample{
				Metric: result.labels,
				Point: promql.Point{
					T: ts,
					V: result.value,
				},
			})
		}
		return next, ts, vec
	}, func() error {
		eg.groups = eg.groups[:0]
		eg.stepResults = eg.stepResults[:0]
		eg.sorted = false
		evaluatorGroupsPool.Put(eg)
		return nextEvaluator.Close()
	}, nextEvaluator.Error)
}

func rangeAggEvaluator(
	it iter.PeekingSampleIterator,
	expr *rangeAggregationExpr,
	q Params,
) (StepEvaluator, error) {
	agg, err := expr.aggregator()
	if err != nil {
		return nil, err
	}
	iter := rangeVectorIteratorPool.Get().(*rangeVectorIterator)
	iter.init(it, expr.left.interval.Nanoseconds(), q.Step().Nanoseconds(), q.Start().UnixNano(), q.End().UnixNano())

	if expr.operation == OpRangeTypeAbsent {
		return &absentRangeVectorEvaluator{
			iter: iter,
			lbs:  absentLabels(expr),
		}, nil
	}
	return &rangeVectorEvaluator{
		iter: iter,
		agg:  agg,
	}, nil
}

type rangeVectorEvaluator struct {
	agg  RangeVectorAggregator
	iter RangeVectorIterator

	err error
}

func (r *rangeVectorEvaluator) Next() (bool, int64, promql.Vector) {
	next := r.iter.Next()
	if !next {
		return false, 0, promql.Vector{}
	}
	ts, vec, hasErrorLabel := r.iter.At(r.agg)
	if hasErrorLabel {
		for _, s := range vec {
			// Errors are not allowed in metrics.
			if s.Metric.Has(log.ErrorLabel) {
				r.err = newPipelineErr(s.Metric)
				return false, 0, promql.Vector{}
			}
		}
	}
	return true, ts, vec
}

func (r rangeVectorEvaluator) Close() error {
	err := r.iter.Close()
	iter := r.iter
	r.iter = nil
	rangeVectorIteratorPool.Put(iter)
	return err
}

func (r rangeVectorEvaluator) Error() error {
	if r.err != nil {
		return r.err
	}
	return r.iter.Error()
}

type absentRangeVectorEvaluator struct {
	iter RangeVectorIterator
	lbs  labels.Labels

	err error
}

func (r *absentRangeVectorEvaluator) Next() (bool, int64, promql.Vector) {
	next := r.iter.Next()
	if !next {
		return false, 0, promql.Vector{}
	}
	ts, vec, hasErrorLabel := r.iter.At(one)
	if hasErrorLabel {
		for _, s := range vec {
			// Errors are not allowed in metrics.
			if s.Metric.Has(log.ErrorLabel) {
				r.err = newPipelineErr(s.Metric)
				return false, 0, promql.Vector{}
			}
		}
	}
	if len(vec) > 0 {
		return next, ts, promql.Vector{}
	}
	// values are missing.
	return next, ts, promql.Vector{
		promql.Sample{
			Point: promql.Point{
				T: ts,
				V: 1.,
			},
			Metric: r.lbs,
		},
	}
}

func (r absentRangeVectorEvaluator) Close() error {
	err := r.iter.Close()
	iter := r.iter
	r.iter = nil
	rangeVectorIteratorPool.Put(iter)
	return err
}

func (r absentRangeVectorEvaluator) Error() error {
	if r.err != nil {
		return r.err
	}
	return r.iter.Error()
}

type binOpStepEvaluator struct {
	ev   SampleEvaluator
	expr *binOpExpr
	lhs  StepEvaluator
	rhs  StepEvaluator
	// q Params
	results promql.Vector
	pairs   map[uint64][2]*promql.Sample
	hashBuf []byte
}

func (b *binOpStepEvaluator) Next() (bool, int64, promql.Vector) {
	// pairs := map[uint64][2]*promql.Sample{}
	var ts int64

	// populate pairs
	{
		// lhs

		next, timestamp, vec := b.lhs.Next()

		ts = timestamp

		// These should _always_ happen at the same step on each evaluator.
		if !next {
			return next, ts, nil
		}

		for _, sample := range vec {
			// TODO(owen-d): this seems wildly inefficient: we're calculating
			// the hash on each sample & step per evaluator.
			// We seem limited to this approach due to using the StepEvaluator ifc.
			var hash uint64
			hash, b.hashBuf = HashLabels(b.hashBuf, sample.Metric)
			pair := b.pairs[hash]
			pair[0] = &promql.Sample{
				Metric: sample.Metric,
				Point:  sample.Point,
			}
			b.pairs[hash] = pair
		}
	}
	{
		// rhs
		next, timestamp, vec := b.rhs.Next()

		ts = timestamp

		// These should _always_ happen at the same step on each evaluator.
		if !next {
			return next, ts, nil
		}

		for _, sample := range vec {
			// TODO(owen-d): this seems wildly inefficient: we're calculating
			// the hash on each sample & step per evaluator.
			// We seem limited to this approach due to using the StepEvaluator ifc.
			var hash uint64
			hash, b.hashBuf = HashLabels(b.hashBuf, sample.Metric)

			pair := b.pairs[hash]
			pair[1] = &promql.Sample{
				Metric: sample.Metric,
				Point:  sample.Point,
			}
			b.pairs[hash] = pair
		}
	}

	if cap(b.results) < len(b.pairs) {
		capResults := 8 * len(b.pairs)
		if capResults < 1024 {
			capResults = 1024
		}
		b.results = make(promql.Vector, len(b.pairs), capResults)
	} else {
		b.results = b.results[:len(b.pairs)]
	}
	// results := make(promql.Vector, len(pairs))
	var iResults int
	for _, pair := range b.pairs {
		// merge
		if merged := mergeBinOp(b.expr.op, pair[0], pair[1], !b.expr.opts.ReturnBool, IsComparisonOperator(b.expr.op), &b.results[iResults]); merged {
			iResults++
		}
	}
	results := b.results[:iResults]
	b.results = b.results[iResults:]
	clear(b.pairs)

	return true, ts, results
}

func (b *binOpStepEvaluator) Close() (lastError error) {
	if err := b.lhs.Close(); err != nil {
		lastError = err
	}
	if err := b.rhs.Close(); err != nil {
		lastError = err
	}
	b.results = nil
	b.pairs = nil
	return lastError
}

func (b *binOpStepEvaluator) Error() error {
	var errs []error
	if err := b.lhs.Error(); err != nil {
		errs = append(errs, err)
	}
	if err := b.rhs.Error(); err != nil {
		errs = append(errs, err)
	}
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return util.MultiError(errs)
	}
}

// newBinOpStepEvaluator explicitly does not handle when both legs are literals as
// it makes the type system simpler and these are reduced in mustNewBinOpExpr
func newBinOpStepEvaluator(
	ctx context.Context,
	ev SampleEvaluator,
	expr *binOpExpr,
	q Params,
) (StepEvaluator, error) {
	// first check if either side is a literal
	leftLit, lOk := expr.SampleExpr.(*literalExpr)
	rightLit, rOk := expr.RHS.(*literalExpr)

	// match a literal expr with all labels in the other leg
	if lOk {
		rhs, err := ev.StepEvaluator(ctx, ev, expr.RHS, q)
		if err != nil {
			return nil, err
		}
		return newLiteralStepEvaluator(
			expr.op,
			leftLit,
			rhs,
			false,
			expr.opts.ReturnBool,
		)
	}
	if rOk {
		lhs, err := ev.StepEvaluator(ctx, ev, expr.SampleExpr, q)
		if err != nil {
			return nil, err
		}
		return newLiteralStepEvaluator(
			expr.op,
			rightLit,
			lhs,
			true,
			expr.opts.ReturnBool,
		)
	}

	// we have two non literal legs
	lhs, err := ev.StepEvaluator(ctx, ev, expr.SampleExpr, q)
	if err != nil {
		return nil, err
	}
	rhs, err := ev.StepEvaluator(ctx, ev, expr.RHS, q)
	if err != nil {
		return nil, err
	}

	return &binOpStepEvaluator{
		ev:    ev,
		expr:  expr,
		lhs:   lhs,
		rhs:   rhs,
		pairs: make(map[uint64][2]*promql.Sample),
	}, nil
}

func mergeBinOp(op string, left, right *promql.Sample, filter, isVectorComparison bool, out *promql.Sample) bool {
	var merger func(left, right, out *promql.Sample) bool

	switch op {
	case OpTypeOr:
		merger = func(left, right, out *promql.Sample) bool {
			// return the left entry found (prefers left hand side)
			if left != nil {
				*out = *left
				return true
			}
			*out = *right
			return true
		}

	case OpTypeAnd:
		merger = func(left, right, out *promql.Sample) bool {
			// return left sample if there's a second sample for that label set
			if left != nil && right != nil {
				*out = *left
				return true
			}
			return false
		}

	case OpTypeUnless:
		merger = func(left, right, out *promql.Sample) bool {
			// return left sample if there's not a second sample for that label set
			if right == nil {
				*out = *left
				return true
			}
			return false
		}

	case OpTypeAdd:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V += right.Point.V
			return true
		}

	case OpTypeSub:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V -= right.Point.V
			return true
		}

	case OpTypeMul:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V *= right.Point.V
			return true
		}

	case OpTypeDiv:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			// guard against divide by zero
			if right.Point.V == 0 {
				out.Point.V = math.NaN()
			} else {
				out.Point.V /= right.Point.V
			}
			return true
		}

	case OpTypeMod:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			// guard against divide by zero
			if right.Point.V == 0 {
				out.Point.V = math.NaN()
			} else {
				out.Point.V = math.Mod(out.Point.V, right.Point.V)
			}
			return true
		}

	case OpTypePow:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = math.Pow(left.Point.V, right.Point.V)
			return true
		}

	case OpTypeCmpEQ:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			val := 0.
			if left.Point.V == right.Point.V {
				val = 1.
			} else if filter {
				return false
			}

			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	case OpTypeNEQ:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}

			val := 0.
			if left.Point.V != right.Point.V {
				val = 1.
			} else if filter {
				return false
			}

			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	case OpTypeGT:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			val := 0.
			if left.Point.V > right.Point.V {
				val = 1.
			} else if filter {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	case OpTypeGTE:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			val := 0.
			if left.Point.V >= right.Point.V {
				val = 1.
			} else if filter {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	case OpTypeLT:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			val := 0.
			if left.Point.V < right.Point.V {
				val = 1.
			} else if filter {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	case OpTypeLTE:
		merger = func(left, right, out *promql.Sample) bool {
			if left == nil || right == nil {
				return false
			}
			val := 0.
			if left.Point.V <= right.Point.V {
				val = 1.
			} else if filter {
				return false
			}
			out.Metric = left.Metric
			out.Point = left.Point
			out.Point.V = val
			return true
		}

	default:
		panic(errors.Errorf("should never happen: unexpected operation: (%s)", op))
	}

	res := merger(left, right, out)
	if !isVectorComparison {
		return res
	}

	if filter {
		// if a filter-enabled vector-wise comparison has returned non-nil,
		// ensure we return the left hand side's value (2) instead of the
		// comparison operator's result (1: the truthy answer)
		if res {
			*out = *left
			return true
		}

		// otherwise it's been filtered out
		return res
	}

	// This only leaves vector comparisons which are not filters.
	// If we could not find a match but we have a left node to compare, create an entry with a 0 value.
	// This can occur when we don't find a matching label set in the vectors.
	if !res && left != nil && right == nil {
		*out = *left
		out.Point.V = 0
		res = true
	}
	return res
}

type literalStepEvaluator struct {
	op           string
	lit          *literalExpr
	eval         StepEvaluator
	inverted     bool
	returnBool   bool
	literalPoint promql.Sample
	results      promql.Vector
}

// newLiteralStepEvaluator merges a literal with a StepEvaluator. Since order matters in
// non commutative operations, inverted should be true when the literalExpr is not the left argument.
func newLiteralStepEvaluator(
	op string,
	lit *literalExpr,
	eval StepEvaluator,
	inverted bool,
	returnBool bool,
) (StepEvaluator, error) {
	if eval == nil {
		return nil, nilStepEvaluatorFnErr
	}
	return &literalStepEvaluator{
		op:         op,
		lit:        lit,
		eval:       eval,
		inverted:   inverted,
		returnBool: returnBool,
	}, nil
}

func (e *literalStepEvaluator) Next() (bool, int64, promql.Vector) {
	ok, ts, vec := e.eval.Next()

	if cap(e.results) < len(vec) {
		capResults := 8 * len(vec)
		if capResults < 1024 {
			capResults = 1024
		}
		e.results = make(promql.Vector, len(vec), capResults)
	} else {
		e.results = e.results[:len(vec)]
	}
	// results := make(promql.Vector, len(vec))
	var iResults int
	for i := range vec {
		e.literalPoint.Metric = vec[i].Metric
		e.literalPoint.Point = promql.Point{T: ts, V: e.lit.value}

		left, right := &e.literalPoint, &vec[i]
		if e.inverted {
			left, right = right, left
		}

		if merged := mergeBinOp(
			e.op,
			left,
			right,
			!e.returnBool,
			IsComparisonOperator(e.op),
			&e.results[iResults],
		); merged {
			iResults++
		}
	}
	results := e.results[:iResults]
	e.results = e.results[iResults:]

	return ok, ts, results
}

func (e *literalStepEvaluator) Close() error {
	e.results = nil
	return e.eval.Close()
}

func (e *literalStepEvaluator) Error() error {
	return e.eval.Error()
}

func labelReplaceEvaluator(
	ctx context.Context,
	ev SampleEvaluator,
	expr *labelReplaceExpr,
	q Params,
) (StepEvaluator, error) {
	nextEvaluator, err := ev.StepEvaluator(ctx, ev, expr.left, q)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1024)
	var labelCache map[uint64]labels.Labels
	return newStepEvaluator(func() (bool, int64, promql.Vector) {
		next, ts, vec := nextEvaluator.Next()
		if !next {
			return false, 0, promql.Vector{}
		}
		if labelCache == nil {
			labelCache = make(map[uint64]labels.Labels, len(vec))
		}
		var hash uint64
		for i, s := range vec {
			hash, buf = HashWithoutLabels(buf, s.Metric)
			// hash, buf = HashLabels(buf, s.Metric)
			// hash, buf = s.Metric.HashWithoutLabels(buf)
			if labels, ok := labelCache[hash]; ok {
				vec[i].Metric = labels
				continue
			}
			src := s.Metric.Get(expr.src)
			indexes := expr.re.FindStringSubmatchIndex(src)
			if indexes == nil {
				// If there is no match, no replacement should take place.
				labelCache[hash] = s.Metric
				continue
			}
			res := expr.re.ExpandString([]byte{}, expr.replacement, src, indexes)

			lb := labels.NewBuilder(s.Metric).Del(expr.dst)
			if len(res) > 0 {
				lb.Set(expr.dst, string(res))
			}
			outLbs := lb.Labels()
			labelCache[hash] = outLbs
			vec[i].Metric = outLbs
		}
		return next, ts, vec
	}, nextEvaluator.Close, nextEvaluator.Error)
}

// This is to replace missing timeseries during absent_over_time aggregation.
func absentLabels(expr SampleExpr) labels.Labels {
	m := labels.Labels{}

	lm := expr.Selector().Matchers()
	if len(lm) == 0 {
		return m
	}

	empty := []string{}
	for _, ma := range lm {
		if ma.Name == labels.MetricName {
			continue
		}
		if ma.Type == labels.MatchEqual && !m.Has(ma.Name) {
			m = labels.NewBuilder(m).Set(ma.Name, ma.Value).Labels()
		} else {
			empty = append(empty, ma.Name)
		}
	}

	for _, v := range empty {
		m = labels.NewBuilder(m).Del(v).Labels()
	}
	return m
}
