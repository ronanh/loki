package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/prometheus/prometheus/promql"
	"github.com/ronanh/loki/logql"
	"github.com/weaveworks/common/httpgrpc"
)

type QueryError string

func (e QueryError) Error() string {
	return string(e)
}

// StatusClientClosedRequest is the status code for when a client request cancellation of an http
// request
const StatusClientClosedRequest = 499

const (
	ErrClientCanceled   = "The request was cancelled by the client."
	ErrDeadlineExceeded = "Request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed."
)

// WriteError write a go error with the correct status code.
func WriteError(err error, w http.ResponseWriter) {
	var (
		queryErr QueryError
		promErr  promql.ErrStorage
	)

	switch {
	case errors.Is(err, context.Canceled) ||
		(errors.As(err, &promErr) && errors.Is(promErr.Err, context.Canceled)):
		http.Error(w, ErrClientCanceled, StatusClientClosedRequest)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, ErrDeadlineExceeded, http.StatusGatewayTimeout)
	case errors.As(err, &queryErr):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, logql.ErrLimit) || errors.Is(err, logql.ErrParse) || errors.Is(err, logql.ErrPipeline):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		if grpcErr, ok := httpgrpc.HTTPResponseFromError(err); ok {
			http.Error(w, string(grpcErr.Body), int(grpcErr.Code))
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
