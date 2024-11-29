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

type rangeVectorIterator struct {
	iter                         iter.PeekingSampleIterator
	selRange, step, end, current int64
	window                       map[string]*wrappedSeries
	metrics                      map[string]wrappedLabels
	at                           []promql.Sample
}

type wrappedLabels struct {
	labels.Labels
	hasErrorLabel bool
}

type wrappedSeries struct {
	Points      []promql.Point
	allocPoints []promql.Point
	wrappedLabels
	createdAt int64
}

// type wrappedSeries struct {
// 	promql.Series
// 	hasErrorLabel bool
// }

func newRangeVectorIterator(
	it iter.PeekingSampleIterator,
	selRange, step, start, end int64) *rangeVectorIterator {
	// forces at least one step.
	if step == 0 {
		step = 1
	}
	return &rangeVectorIterator{
		iter:     it,
		step:     step,
		end:      end,
		selRange: selRange,
		current:  start - step, // first loop iteration will set it to start
		window:   map[string]*wrappedSeries{},
		metrics:  map[string]wrappedLabels{},
	}
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
	for _, s := range r.window {
		putSeries(s)
	}
	r.window = nil
	r.metrics = nil
	return r.iter.Close()
}

func (r *rangeVectorIterator) Error() error {
	return r.iter.Error()
}

// popBack removes all entries out of the current window from the back.
func (r *rangeVectorIterator) popBack(newStart int64) {
	for _, s := range r.window {
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
		series, ok := r.window[lbs]
		if !ok {
			var metric wrappedLabels
			if metric, ok = r.metrics[lbs]; !ok {
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
				r.metrics[lbs] = metric
			}

			series = getSeries()
			series.wrappedLabels = metric
			r.window[lbs] = series
		} else if series.Points == nil {
			series.Points = series.allocPoints
		}
		if len(series.Points) == cap(series.Points) {
			if cap(series.allocPoints) <= 2*len(series.Points) {
				// double the capacity.
				newCap := cap(series.allocPoints) * 2
				if newCap == 0 {
					newCap = 64
				}
				series.allocPoints = make([]promql.Point, 0, newCap)
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
	for _, series := range r.window {
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

var seriesPool sync.Pool

func getSeries() *wrappedSeries {
	if r := seriesPool.Get(); r != nil {
		s := r.(*wrappedSeries)
		s.Points = s.allocPoints
		return s
	}
	allocPoints := make([]promql.Point, 0, 64)
	return &wrappedSeries{
		Points:      allocPoints,
		allocPoints: allocPoints,
		createdAt:   time.Now().UnixNano(),
	}
}

func putSeries(s *wrappedSeries) {
	const maxSeriesCacheDuration = 15 * time.Minute
	if cap(s.allocPoints) > 1<<20 && time.Since(time.Unix(0, s.createdAt)) > maxSeriesCacheDuration {
		return
	}
	s.Points = s.allocPoints
	s.wrappedLabels = wrappedLabels{}
	seriesPool.Put(s)
}
