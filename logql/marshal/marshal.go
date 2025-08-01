// Package marshal converts internal objects to loghttp model objects.  This
// package is designed to work with models in pkg/loghttp.
package marshal

import (
	"io"

	json "github.com/json-iterator/go"
	"github.com/ronanh/loki/loghttp"
	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql"
)

// WriteQueryResponseJSON marshals the promql.Value to v1 loghttp JSON and then
// writes it to the provided io.Writer.
func WriteQueryResponseJSON(v logql.Result, w io.Writer) error {
	value, err := NewResultValue(v.Data)
	if err != nil {
		return err
	}

	q := loghttp.QueryResponse{
		Status: "success",
		Data: loghttp.QueryResponseData{
			ResultType: value.Type(),
			Result:     value,
			Statistics: v.Statistics,
		},
	}

	return json.NewEncoder(w).Encode(q)
}

// WriteLabelResponseJSON marshals a logproto.LabelResponse to v1 loghttp JSON
// and then writes it to the provided io.Writer.
func WriteLabelResponseJSON(l logproto.LabelResponse, w io.Writer) error {
	v1Response := loghttp.LabelResponse{
		Status: "success",
		Data:   l.GetValues(),
	}

	return json.NewEncoder(w).Encode(v1Response)
}

// WebsocketWriter knows how to write message to a websocket connection.
type WebsocketWriter interface {
	WriteMessage(int, []byte) error
}

// WriteSeriesResponseJSON marshals a logproto.SeriesResponse to v1 loghttp JSON and then
// writes it to the provided io.Writer.
func WriteSeriesResponseJSON(r logproto.SeriesResponse, w io.Writer) error {
	adapter := &seriesResponseAdapter{
		Status: "success",
		Data:   make([]map[string]string, 0, len(r.GetSeries())),
	}

	for _, series := range r.GetSeries() {
		adapter.Data = append(adapter.Data, series.GetLabels())
	}

	return json.NewEncoder(w).Encode(adapter)
}

// This struct exists primarily because we can't specify a repeated map in proto v3.
// Otherwise, we'd use that + gogoproto.jsontag to avoid this layer of indirection
type seriesResponseAdapter struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}
