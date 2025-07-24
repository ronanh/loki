package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/ronanh/loki/logql"
)

const (
	ErrDeadlineExceeded = "Request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed."
)

// WriteError write a go error with the correct status code.
func WriteError(err error, w http.ResponseWriter) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, ErrDeadlineExceeded, http.StatusGatewayTimeout)
	case errors.Is(err, logql.ErrLimit) || errors.Is(err, logql.ErrParse) || errors.Is(err, logql.ErrPipeline):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func WriteBadRequestError(err error, w http.ResponseWriter) {
	http.Error(w, err.Error(), http.StatusBadRequest)
}
