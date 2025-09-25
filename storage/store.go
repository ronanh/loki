package storage

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logql"
	model1 "github.com/ronanh/loki/model"
)

// Store is the Loki chunk store to retrieve and save chunks.
type Store interface {
	LabelValuesForMetricName(
		ctx context.Context,
		userID string,
		from, through model.Time,
		metricName string,
		labelName string,
		matchers ...*labels.Matcher,
	) ([]string, error)
	LabelNamesForMetricName(
		ctx context.Context,
		userID string,
		from, through model.Time,
		metricName string,
		matchers ...*labels.Matcher,
	) ([]string, error)

	SelectSamples(ctx context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error)
	SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error)
	GetSeries(ctx context.Context, req logql.SelectLogParams) ([]model1.SeriesIdentifier, error)
}
