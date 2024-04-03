package logql

import (
	"sync"

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
	promql.Series
	hasErrorLabel bool
}

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
	return r.iter.Close()
}

func (r *rangeVectorIterator) Error() error {
	return r.iter.Error()
}

// popBack removes all entries out of the current window from the back.
func (r *rangeVectorIterator) popBack(newStart int64) {
	// possible improvement: if there is no overlap we can just remove all.
	for fp, s := range r.window {
		points := s.Points
		allRemoved := true
		for i := range points {
			if points[i].T > newStart {
				if i > 0 {
					s.Points = points[i:]
				}
				allRemoved = false
				break
			}
		}
		if allRemoved {
			delete(r.window, fp)
			putSeries(s)
		}
	}
}

// load the next sample range window.
func (r *rangeVectorIterator) load(start, end int64) {
	if s, ok := r.iter.(iter.Seekable); ok {
		s.Seek(start)
	}
	var (
		cacheLbs    string
		cacheSeries *wrappedSeries
	)
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
		var series *wrappedSeries
		var ok bool
		if cacheLbs == lbs {
			series = cacheSeries
			ok = true
		} else {
			series, ok = r.window[lbs]
		}
		if !ok {
			var metric wrappedLabels
			if metric, ok = r.metrics[lbs]; !ok {
				if ppl, ok := r.iter.(iter.PeekPromLabels); ok {
					metric = wrappedLabels{ppl.PeekPromLabels(), false}
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
			series.Metric = metric.Labels
			series.hasErrorLabel = metric.hasErrorLabel
			r.window[lbs] = series
		}
		cacheLbs = lbs
		cacheSeries = series
		p := promql.Point{
			T: sample.Timestamp,
			V: sample.Value,
		}
		series.Points = append(series.Points, p)
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
		r.at = append(r.at, promql.Sample{
			Point: promql.Point{
				V: aggregator(series.Points),
				T: ts,
			},
			Metric: series.Metric,
		})
		hasErrorLabel = hasErrorLabel || series.hasErrorLabel
	}
	return ts, r.at, hasErrorLabel
}

var seriesPool sync.Pool

func getSeries() *wrappedSeries {
	if r := seriesPool.Get(); r != nil {
		s := r.(*wrappedSeries)
		s.Points = s.Points[:0]
		return s
	}
	return &wrappedSeries{
		Series: promql.Series{
			Points: make([]promql.Point, 0, 1024),
		},
	}
}

func putSeries(s *wrappedSeries) {
	if cap(s.Points) < 64 {
		return // avoid pooling small slices.
	}
	s.Metric = nil
	seriesPool.Put(s)
}
