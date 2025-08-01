package logql

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ronanh/loki/logproto"
	"github.com/ronanh/loki/logql/stats"
	"github.com/ronanh/loki/sloghandler"
	"github.com/stretchr/testify/require"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

func TestQueryType(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{"bad", "ddd", "", true},
		{"limited", `{app="foo"}`, QueryTypeLimited, false},
		{"limited multi label", `{app="foo" ,fuzz=~"foo"}`, QueryTypeLimited, false},
		{"limited with parser", `{app="foo" ,fuzz=~"foo"} | logfmt`, QueryTypeLimited, false},
		{"filter", `{app="foo"} |= "foo"`, QueryTypeFilter, false},
		{"filter string extracted label", `{app="foo"} | json | foo="a"`, QueryTypeFilter, false},
		{"filter duration", `{app="foo"} | json | duration > 5s`, QueryTypeFilter, false},
		{"metrics", `rate({app="foo"} |= "foo"[5m])`, QueryTypeMetric, false},
		{
			"metrics binary",
			`rate({app="foo"} |= "foo"[5m]) + count_over_time({app="foo"} |= "foo"[5m]) / rate({app="foo"} |= "foo"[5m]) `,
			QueryTypeMetric,
			false,
		},
		{"filters", `{app="foo"} |= "foo" |= "f" != "b"`, QueryTypeFilter, false},
		{
			"filters and labels filters",
			`{app="foo"} |= "foo" |= "f" != "b" | json | a > 5`,
			QueryTypeFilter,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := QueryType(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("QueryType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogSlowQuery(t *testing.T) {
	buf := bytes.NewBufferString("")
	slog.SetDefault(slog.New(sloghandler.New(slog.NewTextHandler(buf, nil))))
	defer slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	ctx := context.Background()
	provider, err := tracesdk.NewProvider()
	require.Nil(t, err)
	ctx, sp := provider.Tracer("test").Start(ctx, "test")

	defer sp.End()

	now := time.Now()

	RecordMetrics(ctx, LiteralParams{
		qs:        `{foo="bar"} |= "buzz"`,
		direction: logproto.BACKWARD,
		end:       now,
		start:     now.Add(-1 * time.Hour),
		limit:     1000,
		step:      time.Minute,
	}, "200", stats.Result{
		Summary: stats.Summary{
			BytesProcessedPerSecond: 100000,
			ExecTime:                25.25,
			TotalBytesProcessed:     100000,
		},
	}, Streams{logproto.Stream{Entries: make([]logproto.Entry, 10)}})
	loggedLine := buf.String()
	require.Contains(t, loggedLine, "level=INFO")
	require.Contains(t, loggedLine, fmt.Sprint("traceID=", sp.SpanContext().TraceID))
	require.Contains(t, loggedLine, "returned_lines=10")
	require.Contains(t, loggedLine, "query=\"{foo=\\\"bar\\\"} |= \\\"buzz\\\"\"")
	require.Contains(t, loggedLine, "query_type=filter")
	require.Contains(t, loggedLine, "range_type=range")
	require.Contains(t, loggedLine, "length=1h0m0s")
	require.Contains(t, loggedLine, "step=1m0s")
	require.Contains(t, loggedLine, "latency=slow")
	require.Contains(
		t,
		loggedLine,
		"duration=25.25s status=200 limit=1000 returned_lines=10 throughput=100kB total_bytes=100kB",
	)
}
