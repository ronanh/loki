package pattern

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Parse(t *testing.T) {
	for _, tc := range []struct {
		input    string
		expected []part
		err      error
	}{
		{
			"<foo> bar f <f>",
			[]part{{capture: []byte("foo")}, {literal: []byte(" bar f ")}, {capture: []byte("f")}},
			nil,
		},
		{
			"<foo<pat>",
			[]part{{literal: []byte("<foo")}, {capture: []byte("pat")}},
			nil,
		},
		{
			"<foo ><bar>",
			[]part{{literal: []byte("<foo >")}, {capture: []byte("bar")}},
			nil,
		},
		{
			"<><pat>",
			[]part{{literal: []byte("<>")}, {capture: []byte("pat")}},
			nil,
		},
		{
			"<_> <pat>",
			[]part{{capture: []byte("_")}, {literal: []byte(" ")}, {capture: []byte("pat")}},
			nil,
		},
		{
			"<1_><pat>",
			[]part{{literal: []byte("<1_>")}, {capture: []byte("pat")}},
			nil,
		},
		{
			`<ip> - <user> [<_>] "<method> <path> <_>" <status> <size> <url> <user_agent>`,
			[]part{{capture: []byte("ip")}, {literal: []byte(" - ")}, {capture: []byte("user")}, {literal: []byte(" [")}, {capture: []byte("_")}, {literal: []byte(`] "`)}, {capture: []byte("method")}, {literal: []byte(" ")}, {capture: []byte("path")}, {literal: []byte(" ")}, {capture: []byte("_")}, {literal: []byte(`" `)}, {capture: []byte("status")}, {literal: []byte(" ")}, {capture: []byte("size")}, {literal: []byte(" ")}, {capture: []byte("url")}, {literal: []byte(" ")}, {capture: []byte("user_agent")}},
			nil,
		},
		{
			"▶<pat>",
			[]part{{literal: []byte("▶")}, {capture: []byte("pat")}},
			nil,
		},
	} {

		t.Run(tc.input, func(t *testing.T) {
			actual, err := CompileFromString(tc.input)
			if tc.err != nil || err != nil {
				require.ErrorIs(t, tc.err, err)
			}
			require.Equal(t, tc.expected, actual.parts)

		})
	}
}

var result *Pattern

func BenchmarkParseExpr(b *testing.B) {
	var err error
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err = CompileFromString(`level=info <_> caller=main.go:107 msg="Starting Grafana Enterprise Traces" version="version=weekly-r138-f1920489, branch=weekly-r138, revision=<revision>"`)
	}
	require.NoError(b, err)
}

func TestMatch(t *testing.T) {

	pat, _ := Compile([]byte("<pipo>ef"))

	iter := pat.Match([]byte("abcdef"))

	assert.Equal(t, 0, iter.currPart)
	assert.Equal(t, 0, iter.pos)
	assert.Equal(t, []byte("abcdef"), iter.input)
	assert.Equal(t, []part{{capture: []byte("pipo")}, {
		literal: []byte("ef"),
	}}, iter.parts)
}

func TestMatchIter(t *testing.T) {
	pat, _ := Compile([]byte("status=<status>,latency=<latency>"))
	iter := pat.Match([]byte("status=200,latency=500ms"))

	replique1, _ := iter.Next()
	replique2, _ := iter.Next()

	assert.Equal(t, MatchItem{
		Key:   []byte("status"),
		Value: []byte("200"),
	}, replique1, "%s/%s", replique1.Key, replique1.Value)
	assert.Equal(t, MatchItem{
		Key:   []byte("latency"),
		Value: []byte("500ms"),
	}, replique2, "%s/%s", replique2.Key, replique2.Value)

	for _, scenario := range []struct {
		name string

		expression string
		line       string

		expected []MatchItem
	}{
		{
			name:       "no matches",
			expression: "wistiti<foo>blah",
			line:       "blah",
			expected:   []MatchItem{},
		},
		{
			name:       "conversation",
			expression: "<replique1>--<replique2>",
			line:       "bonjour, comment ca va?--ca va et toi?",
			expected: []MatchItem{
				{
					Key:   []byte("replique1"),
					Value: []byte("bonjour, comment ca va?"),
				},
				{
					Key:   []byte("replique2"),
					Value: []byte("ca va et toi?"),
				},
			},
		},
		{
			name:       "typical http",
			expression: "status=<status>,latency=<latency>",
			line:       "status=200,latency=500ms",
			expected: []MatchItem{
				{
					Key:   []byte("status"),
					Value: []byte("200"),
				},
				{
					Key:   []byte("latency"),
					Value: []byte("500ms"),
				},
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {

			pat, _ := Compile([]byte(scenario.expression))
			iter := pat.Match([]byte(scenario.line))

			for _, expect := range scenario.expected {
				keyValue, ok := iter.Next()
				assert.True(t, ok)
				assert.Equal(t, expect, keyValue, "%s: %s", keyValue.Key, keyValue.Value)
			}

			kv, ok := iter.Next()
			assert.False(t, ok)
			assert.Equal(t, MatchItem{}, kv)
		})

	}

}

func TestCompile(t *testing.T) {

	for _, scenario := range []struct {
		in  string
		out *Pattern
		err error
	}{
		{
			in: "babab<whatsupp<pat>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("babab<whatsupp"),
					},
					{
						capture: []byte("pat"),
					},
				},
			},
		},
		{
			in:  "ciao<hola>mamamia\\",
			out: nil,
			err: errIncompleteEscape,
		},
		{
			in: "status=<status>,latency=<latency>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("status="),
					},
					{
						capture: []byte("status"),
					},
					{
						literal: []byte(",latency="),
					},
					{
						capture: []byte("latency"),
					},
				},
			},
		},
		{
			in: "<replique1> -- <replique2>",
			out: &Pattern{
				parts: []part{
					{
						capture: []byte("replique1"),
					},
					{
						literal: []byte(" -- "),
					},
					{
						capture: []byte("replique2"),
					},
				},
			},
		},
		{
			in: "<_>abch<tizio>exx<_>",
			out: &Pattern{
				parts: []part{
					{
						capture: []byte("_"),
					},
					{
						literal: []byte("abch"),
					},
					{
						capture: []byte("tizio"),
					},
					{
						literal: []byte("exx"),
					},
					{
						capture: []byte("_"),
					},
				},
			},
		},
		{
			in:  "<_>abch<_>exx<_>",
			err: errZeroNamedCaptures,
		},
		{
			in:  "abchexx",
			err: errZeroNamedCaptures,
		},
		{
			in: "abc<><pat>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc<>"),
					},
					{
						capture: []byte("pat"),
					},
				},
			},
		},
		{
			in: "abc<example>xddd",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc"),
					},
					{
						capture: []byte("example"),
					},
					{
						literal: []byte("xddd"),
					},
				},
			},
		},
		{
			in: "abc<def>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc"),
					},
					{
						capture: []byte("def"),
					},
				},
			},
		},
		{
			in: "sss><pat>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("sss>"),
					},
					{
						capture: []byte("pat"),
					},
				},
			},
		},
		{
			in: ">>huuuh??<<<pat>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte(">>huuuh??<<"),
					},
					{
						capture: []byte("pat"),
					},
				},
			},
		},
		{
			in: "abc<ss<xd>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("abc<ss"),
					},
					{
						capture: []byte("xd"),
					},
				},
			},
		},
		{
			in: "ab\\<c<ssxd>blabla\\>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("ab\\<c"),
					},
					{
						capture: []byte("ssxd"),
					},
					{
						literal: []byte("blabla\\>"),
					},
				},
			},
			err: nil,
		},
		{
			in:  "ab<capture1><capture2>",
			out: nil,
			err: errSuccessiveCapturesNotAllowed,
		},
		{
			in: "ab<capture1> <capture2>",
			out: &Pattern{
				parts: []part{
					{
						literal: []byte("ab"),
					},
					{
						capture: []byte("capture1"),
					},
					{
						literal: []byte(" "),
					},
					{
						capture: []byte("capture2"),
					},
				},
			},
			err: nil,
		},
	} {
		t.Run(scenario.in, func(t *testing.T) {
			v, e := Compile([]byte(scenario.in))
			assert.ErrorIs(t, e, scenario.err)
			assert.Equal(t, scenario.out, v)
		})
	}

}

var fixtures = []struct {
	expr     string
	in       string
	expected []string
	matches  bool
}{
	{
		"foo <foo> bar",
		"foo buzz bar",
		[]string{"buzz"},
		true,
	},
	{
		"foo <foo> bar<fuzz>",
		"foo buzz bar",
		[]string{"buzz", ""},
		false,
	},
	{
		"<foo>foo <bar> bar",
		"foo buzz bar",
		[]string{"", "buzz"},
		false,
	},
	{
		"<foo> bar<fuzz>",
		" bar",
		[]string{"", ""},
		false,
	},
	{
		"<foo>bar<baz>",
		" bar ",
		[]string{" ", " "},
		true,
	},
	{
		"<foo> bar<baz>",
		" bar ",
		[]string{"", " "},
		false,
	},
	{
		"<foo>bar <baz>",
		" bar ",
		[]string{" ", ""},
		false,
	},
	{
		"<foo>",
		" bar ",
		[]string{" bar "},
		true,
	},
	{
		"<path>?<_>",
		`/api/plugins/versioncheck?slugIn=snuids-trafficlights-panel,input,gel&grafanaVersion=7.0.0-beta1`,
		[]string{"/api/plugins/versioncheck"},
		true,
	},
	{
		"<path>?<_>",
		`/api/plugins/status`,
		[]string{"/api/plugins/status"},
		false,
	},
	{
		// Common Log Format
		`<ip> <userid> <user> [<_>] "<method> <path> <_>" <status> <size>`,
		`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
		[]string{"127.0.0.1", "user-identifier", "frank", "GET", "/apache_pb.gif", "200", "2326"},
		true,
	},
	{
		// Combined Log Format
		`<ip> - - [<_>] "<method> <path> <_>" <status> <size> `,
		`35.191.8.106 - - [19/May/2021:07:21:49 +0000] "GET /api/plugins/versioncheck?slugIn=snuids-trafficlights-panel,input,gel&grafanaVersion=7.0.0-beta1 HTTP/1.1" 200 107 "-" "Go-http-client/2.0" "80.153.74.144, 34.120.177.193" "TLSv1.3" "DE" "DEBW"`,
		[]string{"35.191.8.106", "GET", "/api/plugins/versioncheck?slugIn=snuids-trafficlights-panel,input,gel&grafanaVersion=7.0.0-beta1", "200", "107"},
		false,
	},
	{
		// MySQL
		`<_> <id> [<level>] [<no>] [<component>] `,
		`2020-08-06T14:25:02.835618Z 0 [Note] [MY-012487] [InnoDB] DDL log recovery : begin`,
		[]string{"0", "Note", "MY-012487", "InnoDB"},
		false,
	},
	{
		// MySQL
		`<_> <id> [<level>] `,
		`2021-05-19T07:40:12.215792Z 42761518 [Note] Aborted connection 42761518 to db: 'hosted_grafana' user: 'hosted_grafana' host: '10.36.4.122' (Got an error reading communication packets)`,
		[]string{"42761518", "Note"},
		false,
	},
	{
		// Kubernetes api-server
		`<id> <_>       <_> <line>] `,
		`W0519 07:46:47.647050       1 clientconn.go:1223] grpc: addrConn.createTransport failed to connect to {https://kubernetes-etcd-1.kubernetes-etcd:2379  <nil> 0 <nil>}. Err :connection error: desc = "transport: Error while dialing dial tcp 10.32.85.85:2379: connect: connection refused". Reconnecting...`,
		[]string{"W0519", "clientconn.go:1223"},
		false,
	},
	{
		// Cassandra
		`<level>  [<component>]<_> in <duration>.<_>`,
		`INFO  [Service Thread] 2021-05-19 07:40:12,130 GCInspector.java:284 - ParNew GC in 248ms.  CMS Old Gen: 5043436640 -> 5091062064; Par Eden Space: 671088640 -> 0; Par Survivor Space: 70188280 -> 60139760`,
		[]string{"INFO", "Service Thread", "248ms"},
		true,
	},
	{
		// Cortex & Loki distributor
		`<_> msg="<method> <path> (<status>) <duration>"`,
		`level=debug ts=2021-05-19T07:54:26.864644382Z caller=logging.go:66 traceID=7fbb92fd0eb9c65d msg="POST /loki/api/v1/push (204) 1.238734ms"`,
		[]string{"POST", "/loki/api/v1/push", "204", "1.238734ms"},
		true,
	},
	{
		// Etcd
		`<_> <_> <level> | <component>: <_> peer <peer_id> <_> tcp <ip>:<_>`,
		`2021-05-19 08:16:50.181436 W | rafthttp: health check for peer fd8275e521cfb532 could not connect: dial tcp 10.32.85.85:2380: connect: connection refused`,
		[]string{"W", "rafthttp", "fd8275e521cfb532", "10.32.85.85"},
		true,
	},
	{
		// Kafka
		`<_>] <level> [Log partition=<part>, dir=<dir>] `,
		`[2021-05-19 08:35:28,681] INFO [Log partition=p-636-L-fs-117, dir=/data/kafka-logs] Deleting segment 455976081 (kafka.log.Log)`,
		[]string{"INFO", "p-636-L-fs-117", "/data/kafka-logs"},
		false,
	},
	{
		// Elastic
		`<_>][<level>][<component>] [<id>] [<index>]`,
		`[2021-05-19T06:54:06,994][INFO ][o.e.c.m.MetaDataMappingService] [1f605d47-8454-4bfb-a67f-49f318bf837a] [usage-stats-2021.05.19/O2Je9IbmR8CqFyUvNpTttA] update_mapping [report]`,
		[]string{"INFO ", "o.e.c.m.MetaDataMappingService", "1f605d47-8454-4bfb-a67f-49f318bf837a", "usage-stats-2021.05.19/O2Je9IbmR8CqFyUvNpTttA"},
		false,
	},
	{
		// Envoy
		`<_> "<method> <path> <_>" <status> <_> <received_bytes> <sent_bytes> <duration> <upstream_time> "<forward_for>" "<agent>" <_> <_> "<upstream>"`,
		`[2016-04-15T20:17:00.310Z] "POST /api/v1/locations HTTP/2" 204 - 154 0 226 100 "10.0.35.28" "nsq2http" "cc21d9b0-cf5c-432b-8c7e-98aeb7988cd2" "locations" "tcp://10.0.2.1:80"`,
		[]string{"POST", "/api/v1/locations", "204", "154", "0", "226", "100", "10.0.35.28", "nsq2http", "tcp://10.0.2.1:80"},
		true,
	},
	{
		// UTF-8: Matches a unicode character
		`unicode <emoji> character`,
		`unicode 🤷 character`,
		[]string{`🤷`},
		true,
	},
	{
		// UTF-8: Parses unicode character as literal
		"unicode ▶ <what>",
		"unicode ▶ character",
		[]string{"character"},
		true,
	},
}

func Test_BytesIndexUnicode(t *testing.T) {
	data := []byte("Hello ▶ World")
	index := bytes.Index(data, []byte("▶"))
	require.Equal(t, 6, index)
}

func Test_matcher_Matches(t *testing.T) {
	for _, tt := range fixtures {
		t.Run(tt.expr, func(t *testing.T) {
			t.Parallel()
			m, err := CompileFromString(tt.expr)
			require.NoError(t, err)
			line := []byte(tt.in)
			actual := m.Matches(line)
			var actualStrings []string
			for _, a := range actual {
				actualStrings = append(actualStrings, string(a.Value))
			}
			assert.Equal(t, tt.expected, actualStrings)
		})
	}
}

var res []MatchItem

func Benchmark_matcher_Matches(b *testing.B) {
	for _, tt := range fixtures {
		b.Run(tt.expr, func(b *testing.B) {
			b.ReportAllocs()
			m, err := CompileFromString(tt.expr)
			require.NoError(b, err)
			b.ResetTimer()
			l := []byte(tt.in)
			for n := 0; n < b.N; n++ {
				res = m.Matches(l)
			}
		})
	}
}

func Test_Error(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"<f>", nil},
		{"<f> <a>", nil},
		{"<_>", errZeroNamedCaptures},
		{"foo <_> bar <_>", errZeroNamedCaptures},
		{"foo bar buzz", errZeroNamedCaptures},
		{"<f><f>", errSuccessiveCapturesNotAllowed},
		{"<f> f<d><b>", errSuccessiveCapturesNotAllowed},
		{"<f> f<f>", errDuplicateCapture},
		{`f<f><_>`, errSuccessiveCapturesNotAllowed},
		{`<f>f<f><_>`, errDuplicateCapture},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileFromString(tt.name)
			require.ErrorIs(t, err, tt.err)
		})
	}
}
