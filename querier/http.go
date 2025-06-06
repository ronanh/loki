package querier

import (
	"context"
	"net/http"
	"time"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
	kit_log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/websocket"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/ronanh/loki/loghttp"
	loghttp_legacy "github.com/ronanh/loki/loghttp/legacy"
	"github.com/ronanh/loki/logql"
	"github.com/ronanh/loki/logql/marshal"
	serverutil "github.com/ronanh/loki/util/server"
	"github.com/ronanh/loki/util/validation"
)

const (
	wsPingPeriod = 1 * time.Second
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
		engine:  logql.NewEngine(cfg.Engine, q, limits),
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

	if err := q.validateEntriesLimits(ctx, request.Query, request.Limit); err != nil {
		serverutil.WriteError(err, w)
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

	if err := q.validateEntriesLimits(ctx, request.Query, request.Limit); err != nil {
		serverutil.WriteError(err, w)
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

func handleError(conn *websocket.Conn, logger kit_log.Logger, msg string, err error) {
	level.Error(logger).Log("msg", msg, "err", err)
	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
		level.Error(logger).Log("msg", "Error connecting to ingesters for tailing", "err", err)
	}

}

// TailHandler is a http.HandlerFunc for handling tail queries.
func (q *HttpQuerier) TailHandler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	logger := util_log.WithContext(r.Context(), util_log.Logger)

	req, err := loghttp.ParseTailQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	req.Query, err = parseRegexQuery(r)
	if err != nil {
		serverutil.WriteError(httpgrpc.Errorf(http.StatusBadRequest, err.Error()), w)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		level.Error(logger).Log("msg", "Error in upgrading websocket", "err", err)
		return
	}

	defer func() {
		if err := conn.Close(); err != nil {
			level.Error(logger).Log("msg", "Error closing websocket", "err", err)
		}
	}()

	tailer, err := q.querier.Tail(r.Context(), req)
	if err != nil {
		if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
			level.Error(logger).Log("msg", "Error connecting to ingesters for tailing", "err", err)
		}
		return
	}
	defer func() {
		if err := tailer.close(); err != nil {
			level.Error(logger).Log("msg", "Error closing Tailer", "err", err)
		}
	}()

	ticker := time.NewTicker(wsPingPeriod)
	defer ticker.Stop()

	var response *loghttp_legacy.TailResponse
	responseChan := tailer.getResponseChan()
	closeErrChan := tailer.getCloseErrorChan()

	doneChan := make(chan struct{})
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if closeErr, ok := err.(*websocket.CloseError); ok {
					if closeErr.Code == websocket.CloseNormalClosure {
						break
					}
					level.Error(logger).Log("msg", "Error from client", "err", err)
					break
				} else if tailer.stopped {
					return
				} else {
					level.Error(logger).Log("msg", "Unexpected error from client", "err", err)
					break
				}
			}
		}
		doneChan <- struct{}{}
	}()

	for {
		select {
		case response = <-responseChan:
			if err := loghttp.EnsureHasV1(r.RequestURI); err != nil {
				handleError(conn, logger, "The request was not V1", err)
				return
			}
			if err := marshal.WriteTailResponseJSON(*response, conn); err != nil {
				handleError(conn, logger, "Error writing to websocket", err)
				return
			}

		case err := <-closeErrChan:
			level.Error(logger).Log("msg", "Error from iterator", "err", err)
			if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
				level.Error(logger).Log("msg", "Error writing close message to websocket", "err", err)
			}
			return
		case <-ticker.C:
			// This is to periodically check whether connection is active, useful to clean up dead connections when there are no entries to send
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				level.Error(logger).Log("msg", "Error writing ping message to websocket", "err", err)
				if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())); err != nil {
					level.Error(logger).Log("msg", "Error writing close message to websocket", "err", err)
				}
				return
			}
		case <-doneChan:
			return
		}
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

// parseRegexQuery parses regex and query querystring from httpRequest and returns the combined LogQL query.
// This is used only to keep regexp query string support until it gets fully deprecated.
func parseRegexQuery(httpRequest *http.Request) (string, error) {
	query := httpRequest.Form.Get("query")
	regexp := httpRequest.Form.Get("regexp")
	if regexp != "" {
		expr, err := logql.ParseLogSelector(query)
		if err != nil {
			return "", err
		}
		newExpr, err := logql.AddFilterExpr(expr, labels.MatchRegexp, regexp)
		if err != nil {
			return "", err
		}
		query = newExpr.String()
	}
	return query, nil
}

func (q *HttpQuerier) validateEntriesLimits(ctx context.Context, query string, limit uint32) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	expr, err := logql.ParseExpr(query)
	if err != nil {
		return err
	}

	// entry limit does not apply to metric queries.
	if _, ok := expr.(logql.SampleExpr); ok {
		return nil
	}

	maxEntriesLimit := q.limits.MaxEntriesLimitPerQuery(userID)
	if int(limit) > maxEntriesLimit && maxEntriesLimit != 0 {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max entries limit per query exceeded, limit > max_entries_limit (%d > %d)", limit, maxEntriesLimit)
	}
	return nil
}
