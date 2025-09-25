package querier

import (
	"context"
	"flag"
	"time"

	"github.com/ronanh/loki/iter"
	"github.com/ronanh/loki/logql"
	"github.com/ronanh/loki/model"
)

// Config for a querier.
type Config struct {
	QueryTimeout         time.Duration    `yaml:"query_timeout"`
	QueryIngestersWithin time.Duration    `yaml:"query_ingesters_within,omitempty"`
	Engine               logql.EngineOpts `yaml:"engine,omitempty"`
}

// RegisterFlags register flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Engine.RegisterFlagsWithPrefix("querier", f)
	f.DurationVar(
		&cfg.QueryTimeout,
		"querier.query-timeout",
		1*time.Minute,
		"Timeout when querying backends (ingesters or storage) during the execution of a query request",
	)
}

// Querier handlers queries.
type Querier interface {
	SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error)
	SelectSamples(ctx context.Context, params logql.SelectSampleParams) (iter.SampleIterator, error)
	Label(ctx context.Context, req *model.LabelRequest) (*model.LabelResponse, error)
	Series(ctx context.Context, req *model.SeriesRequest) (*model.SeriesResponse, error)
}
