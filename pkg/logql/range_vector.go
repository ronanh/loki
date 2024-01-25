package logql

import (
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promql_parser "github.com/prometheus/prometheus/promql/parser"

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
	At(aggregator RangeVectorAggregator) (int64, promql.Vector)
	Close() error
	Error() error
}

type rangeVectorIterator struct {
	iter                         iter.PeekingSampleIterator
	selRange, step, end, current int64
	window                       map[string]*promql.Series
	metrics                      map[string]labels.Labels
	at                           []promql.Sample
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
		window:   map[string]*promql.Series{},
		metrics:  map[string]labels.Labels{},
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
		nbPointsRemoved := 0
		for _, p := range r.window[fp].Points {
			if p.T > newStart {
				break
			}
			nbPointsRemoved++
		}
		if nbPointsRemoved > 0 {
			// copy + truncate
			copy(s.Points, s.Points[nbPointsRemoved:])
			s.Points = s.Points[:len(s.Points)-nbPointsRemoved]
		}
		if len(s.Points) == 0 {
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
		cacheSeries *promql.Series
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
		var series *promql.Series
		var ok bool
		if cacheLbs == lbs {
			series = cacheSeries
			ok = true
		} else {
			series, ok = r.window[lbs]
		}
		if !ok {
			var metric labels.Labels
			if metric, ok = r.metrics[lbs]; !ok {
				if ppl, ok := r.iter.(iter.PeekPromLabels); ok {
					metric = ppl.PeekPromLabels()
				}
				if len(metric) == 0 {
					var err error
					metric, err = promql_parser.ParseMetric(lbs)
					if err != nil {
						_ = r.iter.Next()
						continue
					}
				}
				r.metrics[lbs] = metric
			}

			series = getSeries()
			series.Metric = metric
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

func (r *rangeVectorIterator) At(aggregator RangeVectorAggregator) (int64, promql.Vector) {
	if r.at == nil {
		r.at = make([]promql.Sample, 0, len(r.window))
	}
	r.at = r.at[:0]
	// convert ts from nano to milli seconds as the iterator work with nanoseconds
	ts := r.current / 1e+6
	for _, series := range r.window {
		r.at = append(r.at, promql.Sample{
			Point: promql.Point{
				V: aggregator(series.Points),
				T: ts,
			},
			Metric: series.Metric,
		})
	}
	return ts, r.at
}

var seriesPool sync.Pool

func getSeries() *promql.Series {
	if r := seriesPool.Get(); r != nil {
		s := r.(*promql.Series)
		s.Points = s.Points[:0]
		return s
	}
	return &promql.Series{
		Points: make([]promql.Point, 0, 1024),
	}
}

func putSeries(s *promql.Series) {
	seriesPool.Put(s)
}
