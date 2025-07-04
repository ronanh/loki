package log

import (
	"sort"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

func Test_lineFormatter_Format2(t *testing.T) {
	tests := []struct {
		name  string
		fmter *LineFormatter
		lbs   labels.Labels

		want    []byte
		wantLbs labels.Labels
		in      []byte
	}{
		{
			"combining",
			newMustLineFormatter("foo{{.foo}}buzz{{  .bar  }}"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("fooblipbuzzblop"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"Replace",
			newMustLineFormatter(`foo{{.foo}}buzz{{ Replace .bar "blop" "bar" -1 }}`),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("fooblipbuzzbar"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"replace",
			newMustLineFormatter(`foo{{.foo}}buzz{{ .bar | replace "blop" "bar" }}`),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("fooblipbuzzbar"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"title",
			newMustLineFormatter(`{{.foo | title }}`),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("Blip"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"substr and trunc",
			newMustLineFormatter(
				`{{.foo | substr 1 3 }} {{ .bar  | trunc 1 }} {{ .bar  | trunc 3 }}`,
			),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("li b blo"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"trim",
			newMustLineFormatter(
				`{{.foo | trim }} {{ .bar  | trimAll "op" }} {{ .bar  | trimPrefix "b" }} {{ .bar  | trimSuffix "p" }}`,
			),
			labels.FromStrings("foo", "  blip ", "bar", "blop"),
			[]byte("blip bl lop blo"),
			labels.FromStrings("foo", "  blip ", "bar", "blop"),
			nil,
		},
		{
			"lower and upper",
			newMustLineFormatter(`{{.foo | lower }} {{ .bar  | upper }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("blip BLOP"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"urlencode",
			newMustLineFormatter(
				`{{.foo | urlencode }} {{ urlencode .foo }}`,
			), // assert both syntax forms
			labels.FromStrings(
				"foo",
				`/loki/api/v1/query?query=sum(count_over_time({stream_filter="some_stream",environment="prod", host=~"someec2.*"}`,
			),
			[]byte(
				"%2Floki%2Fapi%2Fv1%2Fquery%3Fquery%3Dsum%28count_over_time%28%7Bstream_filter%3D%22some_stream%22%2Cenvironment%3D%22prod%22%2C+host%3D~%22someec2.%2A%22%7D %2Floki%2Fapi%2Fv1%2Fquery%3Fquery%3Dsum%28count_over_time%28%7Bstream_filter%3D%22some_stream%22%2Cenvironment%3D%22prod%22%2C+host%3D~%22someec2.%2A%22%7D",
			),
			labels.FromStrings(
				"foo",
				`/loki/api/v1/query?query=sum(count_over_time({stream_filter="some_stream",environment="prod", host=~"someec2.*"}`,
			),
			nil,
		},
		{
			"urldecode",
			newMustLineFormatter(
				`{{.foo | urldecode }} {{ urldecode .foo }}`,
			), // assert both syntax forms
			labels.FromStrings(
				"foo",
				`%2Floki%2Fapi%2Fv1%2Fquery%3Fquery%3Dsum%28count_over_time%28%7Bstream_filter%3D%22some_stream%22%2Cenvironment%3D%22prod%22%2C+host%3D~%22someec2.%2A%22%7D`,
			),
			[]byte(
				`/loki/api/v1/query?query=sum(count_over_time({stream_filter="some_stream",environment="prod", host=~"someec2.*"} /loki/api/v1/query?query=sum(count_over_time({stream_filter="some_stream",environment="prod", host=~"someec2.*"}`,
			),
			labels.FromStrings(
				"foo",
				`%2Floki%2Fapi%2Fv1%2Fquery%3Fquery%3Dsum%28count_over_time%28%7Bstream_filter%3D%22some_stream%22%2Cenvironment%3D%22prod%22%2C+host%3D~%22someec2.%2A%22%7D`,
			),
			nil,
		},
		{
			"repeat",
			newMustLineFormatter(`{{ "foo" | repeat 3 }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("foofoofoo"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"indent",
			newMustLineFormatter(`{{ "foo\n bar" | indent 4 }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("    foo\n     bar"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"nindent",
			newMustLineFormatter(`{{ "foo" | nindent 2 }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("\n  foo"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"contains",
			newMustLineFormatter(
				`{{ if  .foo | contains "p"}}yes{{end}}-{{ if  .foo | contains "z"}}no{{end}}`,
			),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("yes-"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"hasPrefix",
			newMustLineFormatter(
				`{{ if  .foo | hasPrefix "BL" }}yes{{end}}-{{ if  .foo | hasPrefix "p"}}no{{end}}`,
			),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("yes-"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"hasSuffix",
			newMustLineFormatter(
				`{{ if  .foo | hasSuffix "Ip" }}yes{{end}}-{{ if  .foo | hasSuffix "pw"}}no{{end}}`,
			),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("yes-"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"regexReplaceAll",
			newMustLineFormatter(`{{ regexReplaceAll "(p)" .foo "t" }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("BLIt"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"regexReplaceAllLiteral",
			newMustLineFormatter(`{{ regexReplaceAllLiteral "(p)" .foo "${1}" }}`),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			[]byte("BLI${1}"),
			labels.FromStrings("foo", "BLIp", "bar", "blop"),
			nil,
		},
		{
			"err",
			newMustLineFormatter(`{{.foo Replace "foo"}}`),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
			labels.FromStrings("__error__", "TemplateFormatErr",
				"foo", "blip", "bar", "blop",
			),
			nil,
		},
		{
			"missing",
			newMustLineFormatter("foo {{.foo}}buzz{{  .bar  }}"),
			labels.FromStrings("bar", "blop"),
			[]byte("foo buzzblop"),
			labels.FromStrings("bar", "blop"),
			nil,
		},
		{
			"function",
			newMustLineFormatter("foo {{.foo | ToUpper }} buzz{{  .bar  }}"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			[]byte("foo BLIP buzzblop"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
		},
		{
			"mathint",
			newMustLineFormatter("{{ add .foo 1 | sub .bar | mul .baz | div .bazz}}"),
			labels.FromStrings("foo", "1", "bar", "3", "baz", "10", "bazz", "20"),
			[]byte("2"),
			labels.FromStrings("foo", "1", "bar", "3", "baz", "10", "bazz", "20"),
			nil,
		},
		{
			"mathfloat",
			newMustLineFormatter("{{ addf .foo 1.5 | subf .bar 1.5 | mulf .baz | divf .bazz }}"),
			labels.FromStrings("foo", "1.5", "bar", "5", "baz", "10.5", "bazz", "20.2"),
			[]byte("3.8476190476190477"),
			labels.FromStrings("foo", "1.5", "bar", "5", "baz", "10.5", "bazz", "20.2"),
			nil,
		},
		{
			"mathfloatround",
			newMustLineFormatter(
				"{{ round (addf .foo 1.5 | subf .bar | mulf .baz | divf .bazz) 5 .2}}",
			),
			labels.FromStrings("foo", "1.5", "bar", "3.5", "baz", "10.5", "bazz", "20.4"),
			[]byte("3.88572"),
			labels.FromStrings("foo", "1.5", "bar", "3.5", "baz", "10.5", "bazz", "20.4"),
			nil,
		},
		{
			"min",
			newMustLineFormatter(
				"min is {{ min .foo .bar .baz }} and max is {{ max .foo .bar .baz }}",
			),
			labels.FromStrings("foo", "5", "bar", "10", "baz", "15"),
			[]byte("min is 5 and max is 15"),
			labels.FromStrings("foo", "5", "bar", "10", "baz", "15"),
			nil,
		},
		{
			"max",
			newMustLineFormatter(
				"minf is {{ minf .foo .bar .baz }} and maxf is {{maxf .foo .bar .baz}}",
			),
			labels.FromStrings("foo", "5.3", "bar", "10.5", "baz", "15.2"),
			[]byte("minf is 5.3 and maxf is 15.2"),
			labels.FromStrings("foo", "5.3", "bar", "10.5", "baz", "15.2"),
			nil,
		},
		{
			"ceilfloor",
			newMustLineFormatter("ceil is {{ ceil .foo }} and floor is {{floor .foo }}"),
			labels.FromStrings("foo", "5.3"),
			[]byte("ceil is 6 and floor is 5"),
			labels.FromStrings("foo", "5.3"),
			nil,
		},
		{
			"mod",
			newMustLineFormatter("mod is {{ mod .foo 3 }}"),
			labels.FromStrings("foo", "20"),
			[]byte("mod is 2"),
			labels.FromStrings("foo", "20"),
			nil,
		},
		{
			"float64int",
			newMustLineFormatter("{{ \"2.5\" | float64 | int | add 10}}"),
			labels.FromStrings("foo", "2.5"),
			[]byte("12"),
			labels.FromStrings("foo", "2.5"),
			nil,
		},
		{
			"line",
			newMustLineFormatter("{{ __line__ }} bar {{ .bar }}"),
			labels.FromStrings("bar", "2"),
			[]byte("1 bar 2"),
			labels.FromStrings("bar", "2"),
			[]byte("1"),
		},
		{
			"default",
			newMustLineFormatter(
				`{{.foo | default "-" }}{{.bar | default "-"}}{{.unknown | default "-"}}`,
			),
			labels.FromStrings("foo", "blip", "bar", ""),
			[]byte("blip--"),
			labels.FromStrings("foo", "blip", "bar", ""),
			nil,
		},

		{
			"template_error",
			newMustLineFormatter("{{.foo | now}}"),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			nil,
			labels.FromStrings("foo", "blip",
				"bar", "blop",
				"__error__", "TemplateFormatErr",
			),
			nil,
		},
		{
			"bytes 1",
			newMustLineFormatter("{{ .foo | bytes }}"),
			labels.FromStrings("foo", "3 kB"),
			[]byte("3000"),
			labels.FromStrings("foo", "3 kB"),
			[]byte("1"),
		},
		{
			"bytes 2",
			newMustLineFormatter("{{ .foo | bytes }}"),
			labels.FromStrings("foo", "3MB"),
			[]byte("3e+06"),
			labels.FromStrings("foo", "3MB"),
			[]byte("1"),
		},
		{
			"base64encode",
			newMustLineFormatter("{{ .foo | b64enc }}"),
			labels.FromStrings("foo", "i'm a string, encode me!"),
			[]byte("aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh"),
			labels.FromStrings("foo", "i'm a string, encode me!"),
			[]byte("1"),
		},
		{
			"base64decode",
			newMustLineFormatter("{{ .foo | b64dec }}"),
			labels.FromStrings("foo", "aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh"),
			[]byte("i'm a string, encode me!"),
			labels.FromStrings("foo", "aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh"),
			[]byte("1"),
		},
		{
			"simple key template",
			newMustLineFormatter("{{.foo}}"),
			labels.FromStrings("foo", "bar"),
			[]byte("bar"),
			labels.FromStrings("foo", "bar"),
			nil,
		},
		{
			"simple key template with space",
			newMustLineFormatter("{{.foo}}  "),
			labels.FromStrings("foo", "bar"),
			[]byte("bar  "),
			labels.FromStrings("foo", "bar"),
			nil,
		},
		{
			"simple key template with missing key",
			newMustLineFormatter("{{.missing}}"),
			labels.FromStrings("foo", "bar"),
			[]byte{},
			labels.FromStrings("foo", "bar"),
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBaseLabelsBuilder().ForLabels(tt.lbs, tt.lbs.Hash())
			builder.Reset()
			outLine, _ := tt.fmter.Process(tt.in, builder)
			require.Equal(t, tt.want, outLine)
			require.Equal(t, tt.wantLbs, builder.LabelsResult().Labels())
		})
	}
}

func Test_lineFormatter_Format(t *testing.T) {
	tests := []struct {
		name  string
		fmter *LineFormatter
		lbs   labels.Labels

		want    []byte
		wantLbs labels.Labels
	}{
		{
			"combining",
			newMustLineFormatter("foo{{.foo}}buzz{{  .bar  }}"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("fooblipbuzzblop"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
		{
			"Replace",
			newMustLineFormatter(`foo{{.foo}}buzz{{ Replace .bar "blop" "bar" -1 }}`),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("fooblipbuzzbar"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
		{
			"replace",
			newMustLineFormatter(`foo{{.foo}}buzz{{ .bar | replace "blop" "bar" }}`),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("fooblipbuzzbar"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
		{
			"title",
			newMustLineFormatter(`{{.foo | title }}`),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("Blip"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
		{
			"substr and trunc",
			newMustLineFormatter(
				`{{.foo | substr 1 3 }} {{ .bar  | trunc 1 }} {{ .bar  | trunc 3 }}`,
			),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("li b blo"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
		{
			"trim",
			newMustLineFormatter(
				`{{.foo | trim }} {{ .bar  | trimAll "op" }} {{ .bar  | trimPrefix "b" }} {{ .bar  | trimSuffix "p" }}`,
			),
			labels.Labels{{Name: "foo", Value: "  blip "}, {Name: "bar", Value: "blop"}},
			[]byte("blip bl lop blo"),
			labels.Labels{{Name: "foo", Value: "  blip "}, {Name: "bar", Value: "blop"}},
		},
		{
			"lower and upper",
			newMustLineFormatter(`{{.foo | lower }} {{ .bar  | upper }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("blip BLOP"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"repeat",
			newMustLineFormatter(`{{ "foo" | repeat 3 }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("foofoofoo"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"indent",
			newMustLineFormatter(`{{ "foo\n bar" | indent 4 }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("    foo\n     bar"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"nindent",
			newMustLineFormatter(`{{ "foo" | nindent 2 }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("\n  foo"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"contains",
			newMustLineFormatter(
				`{{ if  .foo | contains "p"}}yes{{end}}-{{ if  .foo | contains "z"}}no{{end}}`,
			),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("yes-"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"hasPrefix",
			newMustLineFormatter(
				`{{ if  .foo | hasPrefix "BL" }}yes{{end}}-{{ if  .foo | hasPrefix "p"}}no{{end}}`,
			),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("yes-"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"hasSuffix",
			newMustLineFormatter(
				`{{ if  .foo | hasSuffix "Ip" }}yes{{end}}-{{ if  .foo | hasSuffix "pw"}}no{{end}}`,
			),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("yes-"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"regexReplaceAll",
			newMustLineFormatter(`{{ regexReplaceAll "(p)" .foo "t" }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("BLIt"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"regexReplaceAllLiteral",
			newMustLineFormatter(`{{ regexReplaceAllLiteral "(p)" .foo "${1}" }}`),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
			[]byte("BLI${1}"),
			labels.Labels{{Name: "foo", Value: "BLIp"}, {Name: "bar", Value: "blop"}},
		},
		{
			"err",
			newMustLineFormatter(`{{.foo Replace "foo"}}`),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			nil,
			labels.Labels{
				{Name: ErrorLabel, Value: errTemplateFormat},
				{Name: "foo", Value: "blip"},
				{Name: "bar", Value: "blop"},
			},
		},
		{
			"missing",
			newMustLineFormatter("foo {{.foo}}buzz{{  .bar  }}"),
			labels.Labels{{Name: "bar", Value: "blop"}},
			[]byte("foo buzzblop"),
			labels.Labels{{Name: "bar", Value: "blop"}},
		},
		{
			"function",
			newMustLineFormatter("foo {{.foo | ToUpper }} buzz{{  .bar  }}"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			[]byte("foo BLIP buzzblop"),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sort.Sort(tt.lbs)
			sort.Sort(tt.wantLbs)
			builder := NewBaseLabelsBuilder().ForLabels(tt.lbs, tt.lbs.Hash())
			builder.Reset()
			outLine, _ := tt.fmter.Process(nil, builder)
			require.Equal(t, tt.want, outLine)
			require.Equal(t, tt.wantLbs, builder.LabelsResult().Labels())
		})
	}
}

func newMustLineFormatter(tmpl string) *LineFormatter {
	l, err := NewFormatter(tmpl)
	if err != nil {
		panic(err)
	}
	return l
}

func Test_labelsFormatter_Format(t *testing.T) {
	tests := []struct {
		name  string
		fmter *LabelsFormatter

		in   labels.Labels
		want labels.Labels
	}{
		{
			"combined with template",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("foo", "{{.foo}} and {{.bar}}")}),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			labels.Labels{{Name: "foo", Value: "blip and blop"}, {Name: "bar", Value: "blop"}},
		},
		{
			"combined with template and rename",
			mustNewLabelsFormatter([]LabelFmt{
				NewTemplateLabelFmt("blip", "{{.foo}} and {{.bar}}"),
				NewRenameLabelFmt("bar", "foo"),
			}),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			labels.Labels{{Name: "blip", Value: "blip and blop"}, {Name: "bar", Value: "blip"}},
		},
		{
			"fn",
			mustNewLabelsFormatter([]LabelFmt{
				NewTemplateLabelFmt("blip", "{{.foo | ToUpper }} and {{.bar}}"),
				NewRenameLabelFmt("bar", "foo"),
			}),
			labels.Labels{{Name: "foo", Value: "blip"}, {Name: "bar", Value: "blop"}},
			labels.Labels{{Name: "blip", Value: "BLIP and blop"}, {Name: "bar", Value: "blip"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBaseLabelsBuilder().ForLabels(tt.in, tt.in.Hash())
			builder.Reset()
			_, _ = tt.fmter.Process(nil, builder)
			sort.Sort(tt.want)
			require.Equal(t, tt.want, builder.LabelsResult().Labels())
		})
	}
}

func Test_labelsFormatter_Format2(t *testing.T) {
	tests := []struct {
		name  string
		fmter *LabelsFormatter

		in   labels.Labels
		want labels.Labels
	}{
		{
			"rename label",
			mustNewLabelsFormatter([]LabelFmt{
				NewRenameLabelFmt("baz", "foo"),
			}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("bar", "blop", "baz", "blip"),
		},
		{
			"rename and overwrite existing label",
			mustNewLabelsFormatter([]LabelFmt{
				NewRenameLabelFmt("bar", "foo"),
			}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("bar", "blip"),
		},
		{
			"combined with template",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("foo", "{{.foo}} and {{.bar}}")}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("foo", "blip and blop", "bar", "blop"),
		},
		{
			"combined with template and rename",
			mustNewLabelsFormatter([]LabelFmt{
				NewTemplateLabelFmt("blip", "{{.foo}} and {{.bar}}"),
				NewRenameLabelFmt("bar", "foo"),
			}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("blip", "blip and blop", "bar", "blip"),
		},
		{
			"fn",
			mustNewLabelsFormatter([]LabelFmt{
				NewTemplateLabelFmt("blip", "{{.foo | ToUpper }} and {{.bar}}"),
				NewRenameLabelFmt("bar", "foo"),
			}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("blip", "BLIP and blop", "bar", "blip"),
		},
		{
			"math",
			mustNewLabelsFormatter(
				[]LabelFmt{NewTemplateLabelFmt("status", "{{div .status 100 }}")},
			),
			labels.FromStrings("status", "200"),
			labels.FromStrings("status", "2"),
		},
		{
			"default",
			mustNewLabelsFormatter([]LabelFmt{
				NewTemplateLabelFmt("blip", `{{.foo | default "-" }} and {{.bar}}`),
			}),
			labels.FromStrings("bar", "blop"),
			labels.FromStrings("blip", "- and blop", "bar", "blop"),
		},
		{
			"template error",
			mustNewLabelsFormatter(
				[]LabelFmt{NewTemplateLabelFmt("bar", "{{replace \"test\" .foo}}")},
			),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("foo", "blip",
				"bar", "blop",
				"__error__", "TemplateFormatErr",
			),
		},
		{
			"line",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("line", "{{ __line__ }}")}),
			labels.FromStrings("foo", "blip", "bar", "blop"),
			labels.FromStrings("foo", "blip",
				"bar", "blop",
				"line", "test line",
			),
		},

		{
			"bytes 1",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("bar", "{{ .foo | bytes }}")}),
			labels.FromStrings("foo", "3 kB", "bar", "blop"),
			labels.FromStrings("foo", "3 kB", "bar", "3000"),
		},
		{
			"bytes 2",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("bar", "{{ .foo | bytes }}")}),
			labels.FromStrings("foo", "3MB", "bar", "blop"),
			labels.FromStrings("foo", "3MB", "bar", "3e+06"),
		},

		{
			"base64encode",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("bar", "{{ .foo | b64enc }}")}),
			labels.FromStrings("foo", "i'm a string, encode me!", "bar", "blop"),
			labels.FromStrings("foo", "i'm a string, encode me!",
				"bar", "aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh",
			),
		},
		{
			"base64decode",
			mustNewLabelsFormatter([]LabelFmt{NewTemplateLabelFmt("bar", "{{ .foo | b64dec }}")}),
			labels.FromStrings("foo", "aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh", "bar", "blop"),
			labels.FromStrings("foo", "aSdtIGEgc3RyaW5nLCBlbmNvZGUgbWUh",
				"bar", "i'm a string, encode me!",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBaseLabelsBuilder().ForLabels(tt.in, tt.in.Hash())
			builder.Reset()
			_, _ = tt.fmter.Process([]byte("test line"), builder)
			require.Equal(t, tt.want, builder.LabelsResult().Labels())
		})
	}
}

func mustNewLabelsFormatter(fmts []LabelFmt) *LabelsFormatter {
	lf, err := NewLabelsFormatter(fmts)
	if err != nil {
		panic(err)
	}
	return lf
}

func Test_validate(t *testing.T) {
	tests := []struct {
		name    string
		fmts    []LabelFmt
		wantErr bool
	}{
		{
			"no dup",
			[]LabelFmt{NewRenameLabelFmt("foo", "bar"), NewRenameLabelFmt("bar", "foo")},
			false,
		},
		{
			"dup",
			[]LabelFmt{NewRenameLabelFmt("foo", "bar"), NewRenameLabelFmt("foo", "blip")},
			true,
		},
		{"no error", []LabelFmt{NewRenameLabelFmt(ErrorLabel, "bar")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validate(tt.fmts); (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLineFormatter_RequiredLabelNames(t *testing.T) {
	tests := []struct {
		fmt  string
		want []string
	}{
		{`{{.foo}} and {{.bar}}`, []string{"foo", "bar"}},
		{`{{ .foo | ToUpper | .buzz }} and {{.bar}}`, []string{"foo", "buzz", "bar"}},
		{`{{ regexReplaceAllLiteral "(p)" .foo "${1}" }}`, []string{"foo"}},
		{
			`{{ if  .foo | hasSuffix "Ip" }} {{.bar}} {{end}}-{{ if  .foo | hasSuffix "pw"}}no{{end}}`,
			[]string{"foo", "bar"},
		},
		{`{{with .foo}}{{printf "%q" .}} {{end}}`, []string{"foo"}},
		{
			`{{with .foo}}{{printf "%q" .}} {{else}} {{ .buzz | lower }} {{end}}`,
			[]string{"foo", "buzz"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.fmt, func(t *testing.T) {
			require.Equal(t, tt.want, newMustLineFormatter(tt.fmt).RequiredLabelNames())
		})
	}
}

func TestLabelFormatter_RequiredLabelNames(t *testing.T) {
	tests := []struct {
		name string
		fmts []LabelFmt
		want []string
	}{
		{"rename", []LabelFmt{NewRenameLabelFmt("foo", "bar")}, []string{"bar"}},
		{
			"rename and fmt",
			[]LabelFmt{
				NewRenameLabelFmt("fuzz", "bar"),
				NewTemplateLabelFmt("1", "{{ .foo | ToUpper | .buzz }} and {{.bar}}"),
			},
			[]string{"bar", "foo", "buzz"},
		},
		{
			"fmt",
			[]LabelFmt{
				NewTemplateLabelFmt("1", "{{.blip}}"),
				NewTemplateLabelFmt("2", "{{ .foo | ToUpper | .buzz }} and {{.bar}}"),
			},
			[]string{"blip", "foo", "buzz", "bar"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, mustNewLabelsFormatter(tt.fmts).RequiredLabelNames())
		})
	}
}
