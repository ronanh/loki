package logql

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_Extractor(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{
		`rate( ( {job="mysql"} |="error" !="timeout" ) [10s] )`,
		`absent_over_time( ( {job="mysql"} |="error" !="timeout" ) [10s] )`,
		`sum without(a) ( rate ( ( {job="mysql"} |="error" !="timeout" ) [10s] ) )`,
		`sum by(a) (rate( ( {job="mysql"} |="error" !="timeout" ) [10s] ) )`,
		`sum(count_over_time({job="mysql"}[5m]))`,
		`sum(count_over_time({job="mysql"} | json [5m]))`,
		`sum(count_over_time({job="mysql"} | logfmt [5m]))`,
		`sum(count_over_time({job="mysql"} | regexp "(?P<foo>foo|bar)" [5m]))`,
		`topk(10,sum(rate({region="us-east1"}[5m])) by (name))`,
		`topk by (name)(10,sum(rate({region="us-east1"}[5m])))`,
		`avg( rate( ( {job="nginx"} |= "GET" ) [10s] ) ) by (region)`,
		`avg(min_over_time({job="nginx"} |= "GET" | unwrap foo[10s])) by (region)`,
		`sum by (cluster) (count_over_time({job="mysql"}[5m]))`,
		`sum by (cluster) (count_over_time({job="mysql"}[5m])) / sum by (cluster) (count_over_time({job="postgres"}[5m])) `,
		`
			sum by (cluster) (count_over_time({job="postgres"}[5m])) /
			sum by (cluster) (count_over_time({job="postgres"}[5m])) /
			sum by (cluster) (count_over_time({job="postgres"}[5m]))
			`,
		`sum by (cluster) (count_over_time({job="mysql"}[5m])) / min(count_over_time({job="mysql"}[5m])) `,
		`sum by (job) (
				count_over_time({namespace="tns"} |= "level=error"[5m])
			/
				count_over_time({namespace="tns"}[5m])
			)`,
		`stdvar_over_time({app="foo"} |= "bar" | json | latency >= 250ms or ( status_code < 500 and status_code > 200)
			| line_format "blip{{ .foo }}blop {{.status_code}}" | label_format foo=bar,status_code="buzz{{.bar}}" | unwrap foo [5m])`,
		`sum_over_time({namespace="tns"} |= "level=error" | json |foo>=5,bar<25ms|unwrap latency [5m])`,
		`sum by (job) (
				sum_over_time({namespace="tns"} |= "level=error" | json | foo=5 and bar<25ms | unwrap latency[5m])
			/
				count_over_time({namespace="tns"} | logfmt | label_format foo=bar[5m])
			)`,
		`sum by (job) (
				sum_over_time({namespace="tns"} |= "level=error" | json | foo=5 and bar<25ms | unwrap bytes(latency)[5m])
			/
				count_over_time({namespace="tns"} | logfmt | label_format foo=bar[5m])
			)`,
		`sum by (job) (
				sum_over_time(
					{namespace="tns"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency) [5m]
				)
			/
				count_over_time({namespace="tns"} | logfmt | label_format foo=bar[5m])
			)`,
		`sum_over_time({namespace="tns"} |= "level=error" | json |foo>=5,bar<25ms | unwrap latency | __error__!~".*" | foo >5[5m])`,
		`absent_over_time({namespace="tns"} |= "level=error" | json |foo>=5,bar<25ms | unwrap latency | __error__!~".*" | foo >5[5m])`,
		`absent_over_time({namespace="tns"} |= "level=error" | json [5m])`,
		`sum by (job) (
				sum_over_time(
					{namespace="tns"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency)  | __error__!~".*" [5m]
				)
			/
				count_over_time({namespace="tns"} | logfmt | label_format foo=bar[5m])
			)`,
		`label_replace(
				sum by (job) (
					sum_over_time(
						{namespace="tns"} |= "level=error" | json | avg=5 and bar<25ms | unwrap duration(latency)  | __error__!~".*" [5m]
					)
				/
					count_over_time({namespace="tns"} | logfmt | label_format foo=bar[5m])
				),
				"foo",
				"$1",
				"service",
				"(.*):.*"
			)
			`,
	} {
		t.Run(tc, func(t *testing.T) {
			expr, err := ParseSampleExpr(tc)
			require.Nil(t, err)
			_, err = expr.Extractor()
			require.Nil(t, err)
		})
	}
}

// Building an extractor leaves the query's syntax tree as parsed. xlog's
// alerting keeps a rule's parsed expression for the rule's lifetime and builds
// an extractor from it for every stream, so whatever one build changed fed the
// next. With an unwrap and a `without` clause, the build appended the unwrapped
// label into the clause's own slice and sorted it: the second build of
// `sum without (d, c, b) (... | unwrap aa ...)` grouped without (aa, aa, b, c)
// and kept d.
func Test_ExtractorLeavesGroupingAsParsed(t *testing.T) {
	stream := labels.FromStrings("aa", "1.5", "app", "x", "b", "2", "c", "3", "d", "4", "e", "5")
	want := labels.FromStrings("app", "x", "e", "5")

	for _, query := range []string{
		`sum without (d, c, b) (sum_over_time({app="x"} | unwrap aa [1m]))`,
		`max_over_time({app="x"} | unwrap aa [1m]) without (d, c, b)`,
	} {
		t.Run(query, func(t *testing.T) {
			expr, err := ParseSampleExpr(query)
			require.NoError(t, err)
			parsed := expr.String()

			for range 3 {
				ex, err := expr.Extractor()
				require.NoError(t, err)
				require.Equal(t, parsed, expr.String())

				_, res, ok := ex.ForStream(stream).ProcessString(0, "line")
				require.True(t, ok)
				require.Equal(t, want, res.Labels())
			}
		})
	}
}
