package querier

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/ronanh/loki/loghttp"
	"github.com/ronanh/loki/logql"
	"github.com/ronanh/loki/logql/marshal"
	serverutil "github.com/ronanh/loki/util/server"
	"github.com/ronanh/loki/util/validation"
)

type HttpQuerier struct {
	querier Querier
	cfg     Config
	engine  *logql.Engine
	limits  *validation.Overrides
}

// NewHttpQuerier Create a new HttpQuerier
func NewHttpQuerier(cfg Config, q Querier, limits *validation.Overrides) (*HttpQuerier, error) {
	hq := HttpQuerier{
		querier: q,
		cfg:     cfg,
		engine:  logql.NewEngine(cfg.Engine, q),
		limits:  limits,
	}
	return &hq, nil
}

type QueryResponse struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

// RangeQueryHandler is a http.HandlerFunc for range queries.
func (q *HttpQuerier) RangeQueryHandler(w http.ResponseWriter, r *http.Request) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	request, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Start,
		request.End,
		request.Step,
		request.Interval,
		request.Direction,
		request.Limit,
		request.Shards,
	)
	query := q.engine.Query(params)
	result, err := query.Exec(ctx)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := marshal.WriteQueryResponseJSON(result, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

// InstantQueryHandler is a http.HandlerFunc for instant queries.
func (q *HttpQuerier) InstantQueryHandler(w http.ResponseWriter, r *http.Request) {
	// Enforce the query timeout while querying backends
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(q.cfg.QueryTimeout))
	defer cancel()

	request, err := loghttp.ParseInstantQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Ts,
		request.Ts,
		0,
		0,
		request.Direction,
		request.Limit,
		nil,
	)
	query := q.engine.Query(params)
	result, err := query.Exec(ctx)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := marshal.WriteQueryResponseJSON(result, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

// LabelHandler is a http.HandlerFunc for handling label queries.
func (q *HttpQuerier) LabelHandler(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	resp, err := q.querier.Label(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err := loghttp.EnsureHasV1(r.RequestURI); err != nil {
		serverutil.WriteError(err, w)
		return
	}

	if err = marshal.WriteLabelResponseJSON(*resp, w); err != nil {
		serverutil.WriteError(err, w)
		return
	}
}

// SeriesHandler returns the list of time series that match a certain label set.
// See https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
func (q *HttpQuerier) SeriesHandler(w http.ResponseWriter, r *http.Request) {
	req, err := loghttp.ParseSeriesQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	resp, err := q.querier.Series(r.Context(), req)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}

	err = marshal.WriteSeriesResponseJSON(*resp, w)
	if err != nil {
		serverutil.WriteError(err, w)
		return
	}
}
