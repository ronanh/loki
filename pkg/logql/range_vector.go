package logql

import (
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promql_parser "github.com/prometheus/prometheus/promql/parser"
	"github.com/ronanh/loki/pkg/logql/log"

	"github.com/ronanh/loki/pkg/iter"
)

// RangeVectorAggregator aggregates samples for a given range of samples.
// It receives the current milliseconds timestamp and the list of point within
// the range.
type RangeVectorAggregator func([]promql.Point) float64

// RangeVectorIterator iterates through a range of samples.
// To fetch the current vector use `At` with a `RangeVectorAggregator`.
type RangeVectorIterator interface {
	Next() bool
	At(aggregator RangeVectorAggregator) (int64, promql.Vector, bool)
	Close() error
	Error() error
}

var (
	rangeVectorIteratorPool = sync.Pool{
		New: func() interface{} {
			return &rangeVectorIterator{}
		},
	}
)

type rangeVectorIterator struct {
	iter                         iter.PeekingSampleIterator
	selRange, step, end, current int64
	window                       []wrappedSeries
	metrics                      []wrappedLabels
	at                           []promql.Sample
}

type wrappedLabels struct {
	labels.Labels
	lbs           string
	hasErrorLabel bool
}

type wrappedSeries struct {
	Points      []promql.Point
	allocPoints []promql.Point
	nbGet       int
	createdAt   time.Time
	allocAfter  time.Duration
	wrappedLabels
}

func newRangeVectorIterator(
	it iter.PeekingSampleIterator,
	selRange, step, start, end int64) *rangeVectorIterator {
	// forces at least one step.
	if step == 0 {
		step = 1
	}
	var r rangeVectorIterator
	r.init(it, selRange, step, start, end)
	return &r
}

func (r *rangeVectorIterator) init(
	it iter.PeekingSampleIterator,
	selRange, step, start, end int64) {
	// forces at least one step.
	if step == 0 {
		step = 1
	}
	r.iter = it
	r.selRange = selRange
	r.step = step
	r.end = end
	r.current = start - step // first loop iteration will set it to start
}

func (r *rangeVectorIterator) Next() bool {
	// slides the range window to the next position
	r.current = r.current + r.step
	if r.current > r.end {
		return false
	}
	rangeEnd := r.current
	rangeStart := r.current - r.selRange
	// load samples
	r.popBack(rangeStart)
	r.load(rangeStart, rangeEnd)
	return true
}

func (r *rangeVectorIterator) Close() error {
	for i := range r.window {
		// might reuse the allocated points
		allocPoints := r.window[i].allocPoints
		createdAt := r.window[i].createdAt
		allocAfter := r.window[i].allocAfter
		// reset the series
		r.window[i] = wrappedSeries{
			createdAt:   createdAt,
			allocAfter:  allocAfter,
			allocPoints: allocPoints,
		}
	}
	r.window = r.window[:cap(r.window)]
	for i := range r.window {
		if r.window[i].allocPoints == nil {
			continue
		}
		if !r.window[i].createdAt.IsZero() && time.Since(r.window[i].createdAt) > r.window[i].allocAfter+time.Minute {
			r.window[i].allocPoints = nil
		}
	}
	window := r.window[:0]
	clear(r.metrics)
	metrics := r.metrics[:0]
	// do not clear at, as it makes tests fail.
	// clear(r.at)
	at := r.at[:0]
	iter := r.iter
	// reset the iterator
	*r = rangeVectorIterator{}
	// reuse work buffers (for rangeVectorIteratorPool)
	r.window = window
	r.metrics = metrics
	r.at = at
	return iter.Close()
}

func (r *rangeVectorIterator) Error() error {
	return r.iter.Error()
}

// popBack removes all entries out of the current window from the back.
func (r *rangeVectorIterator) popBack(newStart int64) {
	for i := range r.window {
		s := &r.window[i]
		if len(s.Points) == 0 || s.Points[len(s.Points)-1].T <= newStart {
			// no overlap, remove all points.
			s.Points = nil
			continue
		}
		// search the first point that is greater than the new start.
		i := sortSearch(s.Points, newStart)
		// remove all points before the new start.
		s.Points = s.Points[i:]
		if 2*cap(s.Points) < cap(s.allocPoints) {
			s.Points = append(s.allocPoints, s.Points...)
		}
	}
}

// sortSearch returns the index of the first point that is greater than the given ts.
func sortSearch(points []promql.Point, ts int64) int {
	return sortSearchRec(points, ts, 0, len(points))
}

func sortSearchRec(points []promql.Point, ts int64, start, end int) int {
	if start == end {
		return start
	}
	mid := (start + end) / 2
	if points[mid].T <= ts {
		return sortSearchRec(points, ts, mid+1, end)
	}
	return sortSearchRec(points, ts, start, mid)
}

func (r *rangeVectorIterator) getOrAddSeries(lbs string) (*wrappedSeries, bool) {
	for i := range r.window {
		if r.window[i].lbs == lbs {
			r.window[i].nbGet++
			if i > 0 && r.window[i].nbGet > r.window[i-1].nbGet {
				// move the serie to the start of the window, so that
				// the most used series are found more quickly.
				r.window[i], r.window[i-1] = r.window[i-1], r.window[i]
				i--
			}
			return &r.window[i], false
		}
	}
	// add a new series
	if len(r.window) == cap(r.window) {
		r.window = append(r.window, wrappedSeries{wrappedLabels: wrappedLabels{lbs: lbs}, createdAt: time.Now()})
	} else {
		// reuse
		r.window = r.window[:len(r.window)+1]
		r.window[len(r.window)-1].lbs = lbs
		r.window[len(r.window)-1].Points = r.window[len(r.window)-1].allocPoints
	}

	return &r.window[len(r.window)-1], true
}

func (r *rangeVectorIterator) getOrAddMetrics(lbs string) (*wrappedLabels, bool) {
	for i := range r.metrics {
		if r.metrics[i].lbs == lbs {
			return &r.metrics[i], false
		}
	}

	r.metrics = append(r.metrics, wrappedLabels{lbs: lbs})
	return &r.metrics[len(r.metrics)-1], true
}

// load the next sample range window.
func (r *rangeVectorIterator) load(start, end int64) {
	if s, ok := r.iter.(iter.Seekable); ok {
		s.Seek(start)
	}
	for lbs, sample, hasNext := r.iter.Peek(); hasNext; lbs, sample, hasNext = r.iter.Peek() {
		if sample.Timestamp > end {
			// not consuming the iterator as this belong to another range.
			return
		}
		// the lower bound of the range is not inclusive
		if sample.Timestamp <= start {
			_ = r.iter.Next()
			continue
		}
		// adds the sample.
		series, newSeries := r.getOrAddSeries(lbs)
		if !newSeries && series.Points == nil {
			series.Points = series.allocPoints
		}
		if newSeries {
			metric, newMetric := r.getOrAddMetrics(lbs)
			if newMetric {
				if ppl, ok := r.iter.(iter.PeekPromLabels); ok {
					metric.Labels = ppl.PeekPromLabels()
				}
				if len(metric.Labels) == 0 {
					var err error
					metric.Labels, err = promql_parser.ParseMetric(lbs)
					if err != nil {
						_ = r.iter.Next()
						continue
					}
				}
				metric.hasErrorLabel = metric.Labels.Has(log.ErrorLabel)
			}
			series.wrappedLabels = *metric
		}
		if len(series.Points) == cap(series.Points) {
			if cap(series.allocPoints) <= 2*len(series.Points) {
				// double the capacity.
				newCap := cap(series.allocPoints) * 2
				if newCap == 0 {
					newCap = 64
				}
				series.allocPoints = make([]promql.Point, 0, newCap)
				series.allocAfter = time.Since(series.createdAt)
				series.Points = append(series.allocPoints, series.Points...)
			} else {
				// reuse the allocated points
				series.Points = append(series.allocPoints, series.Points...)
			}
		}
		series.Points = append(series.Points, promql.Point{
			T: sample.Timestamp,
			V: sample.Value,
		})
		_ = r.iter.Next()
	}
}

func (r *rangeVectorIterator) At(aggregator RangeVectorAggregator) (int64, promql.Vector, bool) {
	if r.at == nil {
		r.at = make([]promql.Sample, 0, len(r.window))
	}
	r.at = r.at[:0]
	// convert ts from nano to milli seconds as the iterator work with nanoseconds
	ts := r.current / 1e+6
	var hasErrorLabel bool
	for i := range r.window {
		series := &r.window[i]
		if series.Points == nil { // removed by popBack
			continue
		}
		r.at = append(r.at, promql.Sample{
			Point: promql.Point{
				V: aggregator(series.Points),
				T: ts,
			},
			Metric: series.Labels,
		})
		hasErrorLabel = hasErrorLabel || series.hasErrorLabel
	}
	return ts, r.at, hasErrorLabel
}
