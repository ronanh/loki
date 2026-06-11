package log

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_jsonParser_Parse(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		lbs  labels.Labels
		want labels.Labels
	}{
		{
			"multi depth",
			[]byte(
				`{"app":"foo","namespace":"prod","pod":{"uuid":"foo","deployment":{"ref":"foobar"}}}`,
			),
			labels.EmptyLabels(),
			labels.FromStrings(
				"app",
				"foo",
				"namespace",
				"prod",
				"pod_uuid",
				"foo",
				"pod_deployment_ref",
				"foobar",
			),
		},
		{
			"numeric",
			[]byte(`{"counter":1, "price": {"_net_":5.56909}}`),
			labels.EmptyLabels(),
			labels.FromStrings("counter", "1", "price__net_", "5.56909"),
		},
		{
			"skip arrays",
			[]byte(`{"counter":1, "price": {"net_":["10","20"]}}`),
			labels.EmptyLabels(),
			labels.FromStrings("counter", "1"),
		},
		{
			"bad key replaced",
			[]byte(`{"cou-nter":1}`),
			labels.EmptyLabels(),
			labels.FromStrings("cou_nter", "1"),
		},
		{
			"errors",
			[]byte(`{n}`),
			labels.EmptyLabels(),
			labels.FromStrings(ErrorLabel, errJSON),
		},
		{
			"duplicate extraction",
			[]byte(
				`{"app":"foo","namespace":"prod","pod":{"uuid":"foo","deployment":{"ref":"foobar"}},"next":{"err":false}}`,
			),
			labels.FromStrings("app", "bar"),
			labels.FromStrings(
				"app",
				"bar",
				"app_extracted",
				"foo",
				"namespace",
				"prod",
				"pod_uuid",
				"foo",
				"next_err",
				"false",
				"pod_deployment_ref",
				"foobar",
			),
		},
	}
	for _, tt := range tests {
		j := NewJSONParser()
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, _ = j.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}

func TestJSONExpressionParser(t *testing.T) {
	testLine := []byte(
		`{"app":"foo","field with space":"value","field with ÜFT8👌":"value","null_field":null,"bool_field":false,"namespace":"prod","pod":{"uuid":"foo","deployment":{"ref":"foobar", "params": [1,2,3]}}}`,
	)

	tests := []struct {
		name        string
		line        []byte
		expressions []JSONExpression
		lbs         labels.Labels
		want        labels.Labels
	}{
		{
			"single field",
			testLine,
			[]JSONExpression{
				NewJSONExpr("app", "app"),
			},
			labels.EmptyLabels(),
			labels.FromStrings("app", "foo"),
		},
		{
			"alternate syntax",
			testLine,
			[]JSONExpression{
				NewJSONExpr("test", `["field with space"]`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("test", "value"),
		},
		{
			"multiple fields",
			testLine,
			[]JSONExpression{
				NewJSONExpr("app", "app"),
				NewJSONExpr("namespace", "namespace"),
			},
			labels.EmptyLabels(),
			labels.FromStrings("app", "foo", "namespace", "prod"),
		},
		{
			"utf8",
			testLine,
			[]JSONExpression{
				NewJSONExpr("utf8", `["field with ÜFT8👌"]`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("utf8", "value"),
		},
		{
			"nested field",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", "pod.uuid"),
			},
			labels.EmptyLabels(),
			labels.FromStrings("uuid", "foo"),
		},
		{
			"nested field alternate syntax",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", `pod["uuid"]`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("uuid", "foo"),
		},
		{
			"nested field alternate syntax 2",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", `["pod"]["uuid"]`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("uuid", "foo"),
		},
		{
			"nested field alternate syntax 3",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", `["pod"].uuid`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("uuid", "foo"),
		},
		{
			"array element",
			testLine,
			[]JSONExpression{
				NewJSONExpr("param", `pod.deployment.params[0]`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("param", "1"),
		},
		{
			"full array",
			testLine,
			[]JSONExpression{
				NewJSONExpr("params", `pod.deployment.params`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("params", "[1,2,3]"),
		},
		{
			"full object",
			testLine,
			[]JSONExpression{
				NewJSONExpr("deployment", `pod.deployment`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("deployment", `{"ref":"foobar", "params": [1,2,3]}`),
		},
		{
			"expression matching nothing",
			testLine,
			[]JSONExpression{
				NewJSONExpr("nope", `pod.nope`),
			},
			labels.EmptyLabels(),
			// empty extracted value: label is dropped (upstream semantics)
			labels.EmptyLabels(),
		},
		{
			"null field",
			testLine,
			[]JSONExpression{
				NewJSONExpr("nf", `null_field`),
			},
			labels.EmptyLabels(),
			// null is coerced to an empty string, which deletes the label
			labels.EmptyLabels(),
		},
		{
			"boolean field",
			testLine,
			[]JSONExpression{
				NewJSONExpr("bool", `bool_field`),
			},
			labels.EmptyLabels(),
			labels.FromStrings("bool", `false`),
		},
		{
			"label override",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", `pod.uuid`),
			},
			labels.FromStrings("uuid", "bar"),
			labels.FromStrings("uuid", "bar", "uuid_extracted", "foo"),
		},
		{
			"non-matching expression",
			testLine,
			[]JSONExpression{
				NewJSONExpr("request_size", `request.size.invalid`),
			},
			labels.FromStrings("uuid", "bar"),
			labels.FromStrings("uuid", "bar"),
		},
		{
			"empty line",
			[]byte("{}"),
			[]JSONExpression{
				NewJSONExpr("uuid", `pod.uuid`),
			},
			labels.EmptyLabels(),
			// empty extracted value: label is dropped (upstream semantics)
			labels.EmptyLabels(),
		},
		{
			"existing labels are not affected",
			testLine,
			[]JSONExpression{
				NewJSONExpr("uuid", `will.not.work`),
			},
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar"),
		},
		{
			"invalid JSON line",
			[]byte(`invalid json`),
			[]JSONExpression{
				NewJSONExpr("uuid", `will.not.work`),
			},
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", ErrorLabel, errJSON),
		},
	}
	for _, tt := range tests {
		j, err := NewJSONExpressionParser(tt.expressions)
		if err != nil {
			t.Fatalf("cannot create JSON expression parser: %s", err.Error())
		}

		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, _ = j.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}

func TestJSONExpressionParserFailures(t *testing.T) {
	tests := []struct {
		name       string
		expression JSONExpression
		error      string
	}{
		{
			"invalid field name",
			NewJSONExpr("app", `field with space`),
			"unexpected FIELD",
		},
		{
			"missing opening square bracket",
			NewJSONExpr("app", `"pod"]`),
			"unexpected STRING, expecting LSB or FIELD",
		},
		{
			"missing closing square bracket",
			NewJSONExpr("app", `["pod"`),
			"unexpected $end, expecting RSB",
		},
		{
			"missing closing square bracket",
			NewJSONExpr("app", `["pod""uuid"]`),
			"unexpected STRING, expecting RSB",
		},
		{
			"invalid nesting",
			NewJSONExpr("app", `pod..uuid`),
			"unexpected DOT, expecting FIELD",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewJSONExpressionParser([]JSONExpression{tt.expression})

			require.NotNil(t, err)
			require.Equal(
				t,
				err.Error(),
				fmt.Sprintf(
					"cannot parse expression [%s]: syntax error: %s",
					tt.expression.Expression,
					tt.error,
				),
			)
		})
	}
}

func Benchmark_Parser(b *testing.B) {
	lbs := labels.FromStrings(
		"cluster",
		"qa-us-central1",
		"namespace",
		"qa",
		"filename",
		"/var/log/pods/ingress-nginx_nginx-ingress-controller-7745855568-blq6t_1f8962ef-f858-4188-a573-ba276a3cacc3/ingress-nginx/0.log",
		"job",
		"ingress-nginx/nginx-ingress-controller",
		"name",
		"nginx-ingress-controller",
		"pod",
		"nginx-ingress-controller-7745855568-blq6t",
		"pod_template_hash",
		"7745855568",
		"stream",
		"stdout",
	)

	jsonLine := `{"proxy_protocol_addr": "","remote_addr": "3.112.221.14","remote_user": "","upstream_addr": "10.12.15.234:5000","the_real_ip": "3.112.221.14","timestamp": "2020-12-11T16:20:07+00:00","protocol": "HTTP/1.1","upstream_name": "hosted-grafana-hosted-grafana-api-80","request": {"id": "c8eacb6053552c0cd1ae443bc660e140","time": "0.001","method" : "GET","host": "hg-api-qa-us-central1.grafana.net","uri": "/","size" : "128","user_agent": "worldping-api","referer": ""},"response": {"status": 200,"upstream_status": "200","size": "1155","size_sent": "265","latency_seconds": "0.001"}}`
	logfmtLine := `level=info ts=2020-12-14T21:25:20.947307459Z caller=metrics.go:83 org_id=29 traceID=c80e691e8db08e2 latency=fast query="sum by (object_name) (rate(({container=\"metrictank\", cluster=\"hm-us-east2\"} |= \"PANIC\")[5m]))" query_type=metric range_type=range length=5m0s step=15s duration=322.623724ms status=200 throughput=1.2GB total_bytes=375MB`
	nginxline := `10.1.0.88 - - [14/Dec/2020:22:56:24 +0000] "GET /static/img/about/bob.jpg HTTP/1.1" 200 60755 "https://grafana.com/go/observabilitycon/grafana-the-open-and-composable-observability-platform/?tech=ggl-o&pg=oss-graf&plcmt=hero-txt" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15" "123.123.123.123, 35.35.122.223" "TLSv1.3"`
	packedLike := `{"job":"123","pod":"someuid123","app":"foo","_entry":"10.1.0.88 - - [14/Dec/2020:22:56:24 +0000] "GET /static/img/about/bob.jpg HTTP/1.1"}`

	for _, tt := range []struct {
		name            string
		line            string
		s               Stage
		LabelParseHints []string //  hints to reduce label extractions.
	}{
		{"json", jsonLine, NewJSONParser(), []string{"response_latency_seconds"}},
		{"unpack", packedLike, NewUnpackParser(), []string{"pod"}},
		{"logfmt", logfmtLine, NewLogfmtParser(), []string{"info", "throughput", "org_id"}},
		{"regex greedy", nginxline, mustNewRegexParser(`GET (?P<path>.*?)/\?`), []string{"path"}},
		{"regex status digits", nginxline, mustNewRegexParser(`HTTP/1.1" (?P<statuscode>\d{3}) `), []string{"statuscode"}},
		{"pattern simple", nginxline, mustNewPatternParser(`<_> HTTP/1.1" <statuscode>`), []string{"statuscode"}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			line := []byte(tt.line)
			b.Run("no labels hints", func(b *testing.B) {
				builder := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
				for n := 0; n < b.N; n++ {
					builder.Reset()
					_, _ = tt.s.Process(0, line, builder)
				}
			})

			b.Run("labels hints", func(b *testing.B) {
				builder := NewBaseLabelsBuilder().ForLabels(lbs, labels.StableHash(lbs))
				builder.parserKeyHints = newParserHint(
					tt.LabelParseHints,
					tt.LabelParseHints,
					false,
					false,
					"",
				)
				for n := 0; n < b.N; n++ {
					builder.Reset()
					_, _ = tt.s.Process(0, line, builder)
				}
			})
		})
	}
}

func TestNewRegexpParser(t *testing.T) {
	tests := []struct {
		name    string
		re      string
		wantErr bool
	}{
		{"no sub", "w.*", true},
		{"sub but not named", "f(.*) (foo|bar|buzz)", true},
		{"named and unamed", "blah (.*) (?P<foo>)", false},
		{"named", "blah (.*) (?P<foo>foo)(?P<bar>barr)", false},
		{"invalid name", "blah (.*) (?P<foo$>foo)(?P<bar>barr)", true},
		{"duplicate", "blah (.*) (?P<foo>foo)(?P<foo>barr)", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRegexpParser(tt.re)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRegexpParser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_regexpParser_Parse(t *testing.T) {
	tests := []struct {
		name   string
		parser *RegexpParser
		line   []byte
		lbs    labels.Labels
		want   labels.Labels
	}{
		{
			"no matches",
			mustNewRegexParser("(?P<foo>foo|bar)buzz"),
			[]byte("blah"),
			labels.FromStrings("app", "foo"),
			labels.FromStrings("app", "foo"),
		},
		{
			"double matches",
			mustNewRegexParser("(?P<foo>.*)buzz"),
			[]byte("matchebuzz barbuzz"),
			labels.FromStrings("app", "bar"),
			labels.FromStrings("app", "bar", "foo", "matchebuzz bar"),
		},
		{
			"duplicate labels",
			mustNewRegexParser("(?P<bar>bar)buzz"),
			[]byte("barbuzz"),
			labels.FromStrings("bar", "foo"),
			labels.FromStrings("bar", "foo", "bar_extracted", "bar"),
		},
		{
			"multiple labels extracted",
			mustNewRegexParser("status=(?P<status>\\w+),latency=(?P<latency>\\w+)(ms|ns)"),
			[]byte("status=200,latency=500ms"),
			labels.FromStrings("app", "foo"),
			labels.FromStrings("app", "foo", "status", "200", "latency", "500"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, _ = tt.parser.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}

func Test_patternParser_Parse(t *testing.T) {
	tests := []struct {
		name   string
		parser *PatternParser
		line   []byte
		lbs    labels.Labels
		want   labels.Labels
	}{
		{
			"no matches",
			mustNewPatternParser("wistiti<foo>buzz"),
			[]byte("blah"),
			labels.FromStrings("app", "foo"),
			labels.FromStrings("app", "foo"),
		},
		{
			"double matches",
			mustNewPatternParser("<foo>buzz"),
			[]byte("matchebuzz barbuzz"),
			labels.FromStrings("app", "bar"),
			labels.FromStrings("app", "bar", "foo", "matche"),
		},
		{
			"duplicate labels",
			mustNewPatternParser("<bar>buzz"),
			[]byte("barbuzz"),
			labels.FromStrings("bar", "foo"),
			labels.FromStrings("bar", "foo", "bar_extracted", "bar"),
		},
		{
			"multiple labels extracted",
			mustNewPatternParser("status=<status>,latency=<latency>ms"),
			[]byte("status=200,latency=500ms"),
			labels.FromStrings("app", "foo"),
			labels.FromStrings("app", "foo", "status", "200", "latency", "500"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, _ = tt.parser.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}

func Test_logfmtParser_Parse(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		lbs  labels.Labels
		want labels.Labels
	}{
		{
			"not logfmt",
			[]byte("foobar====wqe=sdad1r"),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", ErrorLabel, errLogfmt),
		},
		{
			"key alone logfmt",
			[]byte("buzz bar=foo"),
			labels.FromStrings("foo", "bar"),
			// key without value ("buzz") is dropped (upstream semantics)
			labels.FromStrings("foo", "bar", "bar", "foo"),
		},
		{
			"quoted logfmt",
			[]byte(`foobar="foo bar"`),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", "foobar", "foo bar"),
		},
		{
			"double property logfmt",
			[]byte(`foobar="foo bar" latency=10ms`),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", "foobar", "foo bar", "latency", "10ms"),
		},
		{
			"duplicate from line property",
			[]byte(`foobar="foo bar" foobar=10ms`),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", "foobar", "10ms"),
		},
		{
			"duplicate property",
			[]byte(`foo="foo bar" foobar=10ms`),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar", "foo_extracted", "foo bar", "foobar", "10ms"),
		},
		{
			"invalid key names",
			[]byte(`foo="foo bar" foo.bar=10ms test-dash=foo`),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings(
				"foo",
				"bar",
				"foo_extracted",
				"foo bar",
				"foo_bar",
				"10ms",
				"test_dash",
				"foo",
			),
		},
		{
			"nil",
			nil,
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar"),
		},
	}
	p := NewLogfmtParser()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			_, _ = p.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}

func Test_unpackParser_Parse(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		lbs  labels.Labels

		wantLbs  labels.Labels
		wantLine []byte
	}{
		{
			"should extract only map[string]string",
			[]byte(
				`{"bar":1,"app":"foo","namespace":"prod","_entry":"some message","pod":{"uid":"1"}}`,
			),
			labels.FromStrings("cluster", "us-central1"),
			labels.FromStrings("app", "foo", "namespace", "prod", "cluster", "us-central1"),
			[]byte(`some message`),
		},
		{
			"wrong json",
			[]byte(`"app":"foo","namespace":"prod","_entry":"some message","pod":{"uid":"1"}`),
			labels.EmptyLabels(),
			labels.FromStrings("__error__", "JSONParserErr"),
			[]byte(`"app":"foo","namespace":"prod","_entry":"some message","pod":{"uid":"1"}`),
		},
		{
			"not a map",
			[]byte(`["foo","bar"]`),
			labels.FromStrings("cluster", "us-central1"),
			labels.FromStrings("__error__", "JSONParserErr", "cluster", "us-central1"),
			[]byte(`["foo","bar"]`),
		},
		{
			"should rename",
			[]byte(
				`{"bar":1,"app":"foo","namespace":"prod","_entry":"some message","pod":{"uid":"1"}}`,
			),
			labels.FromStrings("cluster", "us-central1", "app", "bar"),
			labels.FromStrings(
				"app",
				"bar",
				"app_extracted",
				"foo",
				"namespace",
				"prod",
				"cluster",
				"us-central1",
			),
			[]byte(`some message`),
		},
		{
			"should not change log and labels if no packed entry",
			[]byte(`{"bar":1,"app":"foo","namespace":"prod","pod":{"uid":"1"}}`),
			labels.FromStrings("app", "bar", "cluster", "us-central1"),
			labels.FromStrings("app", "bar", "cluster", "us-central1"),
			[]byte(`{"bar":1,"app":"foo","namespace":"prod","pod":{"uid":"1"}}`),
		},
		{
			"non json with escaped quotes",
			[]byte(
				`{"_entry":"I0303 17:49:45.976518    1526 kubelet_getters.go:178] \"Pod status updated\" pod=\"openshift-etcd/etcd-ip-10-0-150-50.us-east-2.compute.internal\" status=Running"}`,
			),
			labels.FromStrings("app", "bar", "cluster", "us-central1"),
			labels.FromStrings("app", "bar", "cluster", "us-central1"),
			[]byte(
				`I0303 17:49:45.976518    1526 kubelet_getters.go:178] "Pod status updated" pod="openshift-etcd/etcd-ip-10-0-150-50.us-east-2.compute.internal" status=Running`,
			),
		},
	}
	for _, tt := range tests {
		j := NewUnpackParser()
		t.Run(tt.name, func(t *testing.T) {
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			copy := string(tt.line)
			l, _ := j.Process(0, tt.line, b)
			require.Equal(t, tt.wantLbs, b.LabelsResult().Labels())
			require.Equal(t, tt.wantLine, l)
			require.Equal(t, string(tt.wantLine), string(l))
			require.Equal(t, copy, string(tt.line), "the original log line should not be mutated")
		})
	}
}

func Test_PatternParser(t *testing.T) {
	tests := []struct {
		pattern string
		line    []byte
		lbs     labels.Labels
		want    labels.Labels
	}{
		{
			`<ip> <userid> <user> [<_>] "<method> <path> <_>" <status> <size>`,
			[]byte(
				`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
			),
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("foo", "bar",
				"ip", "127.0.0.1",
				"userid", "user-identifier",
				"user", "frank",
				"method", "GET",
				"path", "/apache_pb.gif",
				"status", "200",
				"size", "2326",
			),
		},
		{
			`<_> msg="<method> <path> (<status>) <duration>"`,
			[]byte(
				`level=debug ts=2021-05-19T07:54:26.864644382Z caller=logging.go:66 traceID=7fbb92fd0eb9c65d msg="POST /loki/api/v1/push (204) 1.238734ms"`,
			),
			labels.FromStrings("method", "bar"),
			labels.FromStrings("method", "bar",
				"method_extracted", "POST",
				"path", "/loki/api/v1/push",
				"status", "204",
				"duration", "1.238734ms",
			),
		},
		{
			`foo <f>"`,
			[]byte(`bar`),
			labels.FromStrings("method", "bar"),
			labels.FromStrings("method", "bar"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()
			b := NewBaseLabelsBuilder().ForLabels(tt.lbs, labels.StableHash(tt.lbs))
			b.Reset()
			pp, err := NewPatternParser(tt.pattern)
			require.NoError(t, err)
			_, _ = pp.Process(0, tt.line, b)
			require.Equal(t, tt.want, b.LabelsResult().Labels())
		})
	}
}
